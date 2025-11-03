# Distributed Benefits Calculator — Slurm submission

# 0) SETUP
library(yaml)
library(data.table)
library(compiler)
library(future)
library(future.apply)
library(future.batchtools)

data.table::setDTthreads(1)
options(future.delete = TRUE)

setwd("//nfs/turbo/bus-omartian/policy-rules-database-main")
root_dir <- getwd()
cfg_dir  <- file.path(root_dir, "configs")
out_dir  <- file.path(root_dir, "outputq")
dir.create(out_dir, showWarnings = FALSE, recursive = TRUE)

# 1) INITIALIZATION
init_env <- function() {
  root_dir <- getwd()
  
  # Load parameter RData into the global environment
  load(file.path(root_dir, "prd_parameters/expenses.rdata"),             envir = globalenv())
  load(file.path(root_dir, "prd_parameters/benefit.parameters.rdata"),   envir = globalenv())
  load(file.path(root_dir, "prd_parameters/tables.rdata"),               envir = globalenv())
  load(file.path(root_dir, "prd_parameters/parameters.defaults.rdata"),  envir = globalenv())
  
  # Unpack defaults
  if (exists("parameters.defaults", envir = globalenv())) {
    list2env(get("parameters.defaults", envir = globalenv()), envir = globalenv())
  }
  
  # Load function scripts into .GlobalEnv
  source(file.path(root_dir, "libraries.R"),                               local = FALSE)
  source(file.path(root_dir, "functions/benefits_functions.R"),            local = FALSE)
  source(file.path(root_dir, "functions/expense_functions.R"),             local = FALSE)
  source(file.path(root_dir, "functions/BenefitsCalculator_functions.R"),  local = FALSE)
  source(file.path(root_dir, "functions/TANF.R"),                           local = FALSE)
  source(file.path(root_dir, "functions/CCDF.R"),                           local = FALSE)
  
  # JIT-compile hot functions
  hot_funcs <- c(
    "function.createData","BenefitsCalculator.ALICEExpenses","BenefitsCalculator.OtherBenefits",
    "BenefitsCalculator.Childcare","BenefitsCalculator.Healthcare","BenefitsCalculator.FoodandHousing",
    "BenefitsCalculator.TaxesandTaxCredits","function.createVars"
  )
  for (fn in hot_funcs) if (exists(fn, mode = "function")) {
    assign(fn, compiler::cmpfun(get(fn)), envir = .GlobalEnv)
  }
  
  assign(".__INIT_DONE__", TRUE, .GlobalEnv)
  invisible(TRUE)
}
# Initialize once in the master
init_env()

# 2) HELPERS
`%||%`    <- function(x, y) if (is.null(x)) y else x
scalarize <- function(x) if (is.list(x) && length(x) == 1L && !is.list(x[[1]])) x[[1]] else x

# Normalize NAs for inspection
sanitize_df <- function(x) {
  stopifnot(is.data.frame(x))
  dt <- data.table::as.data.table(data.table::copy(x))
  for (nm in names(dt)) {
    col <- dt[[nm]]
    if (is.logical(col)) {
      data.table::set(dt, j = nm, value = ifelse(is.na(col), FALSE, col))
    } else if (is.character(col)) {
      data.table::set(dt, j = nm, value = ifelse(is.na(col), "", col))
    } else if (is.numeric(col)) {
      data.table::set(dt, j = nm, value = ifelse(is.na(col), 0, col))
    }
  }
  dt
}

# Food & Housing with LIHEAP safe fallback
safe_FoodAndHousing <- function(df, flags, log_file = file.path(out_dir, "liheap_fallback.log")) {
  tryCatch(
    BenefitsCalculator.FoodandHousing(
      df, flags$SEC8, flags$LIHEAP, flags$SNAP, flags$SLP, flags$WIC, flags$RAP, flags$FRSP
    ),
    error = function(e) {
      # Log to file
      try(write(sprintf("[%s] LIHEAP errored for state=%s; fallback to FALSE. Reason: %s",
                        format(Sys.time(), "%Y-%m-%d %H:%M:%S"),
                        paste(na.omit(unique(df$stateAbbrev)), collapse = ","),
                        e$message),
                file = log_file, append = TRUE), silent = TRUE)
      BenefitsCalculator.FoodandHousing(
        df, flags$SEC8, FALSE, flags$SNAP, flags$SLP, flags$WIC, flags$RAP, flags$FRSP
      )
    }
  )
}

# 3) OUTPUT COLUMN LIST
cols <- c(
  "household_id","ruleYear","stateFIPS","stateName","stateAbbrev","countyortownName",
  "famsize","numadults","numkids", paste0("agePerson",1:12),
  "empl_healthcare","income","assets.cash",
  "exp.childcare","exp.food","exp.rentormortgage","exp.healthcare","exp.utilities","exp.misc","exp.transportation",
  "netexp.childcare","netexp.food","netexp.rentormortgage","netexp.healthcare","netexp.utilities",
  "value.snap","value.schoolmeals","value.section8","value.liheap",
  "value.medicaid.adult","value.medicaid.child","value.aca","value.employerhealthcare",
  "value.CCDF","value.HeadStart","value.PreK",
  "value.cdctc.fed","value.cdctc.state","value.ctc.fed","value.ctc.state",
  "value.eitc.fed","value.eitc.state","value.eitc","value.ctc","value.cdctc",
  "value.ssdi","value.ssi","value.tanf","AfterTaxIncome","NetResources",
  "tax.income.fed","tax.income.state"
)

# 4) LOAD YAML CONFIGS
config_files <- list.files(cfg_dir, "\\.yml$", full.names = TRUE)
hh_ids       <- tools::file_path_sans_ext(basename(config_files))
yaml_list    <- setNames(lapply(config_files, yaml::read_yaml), hh_ids)

flatten_list <- function(x, parent = NULL) {
  out <- list()
  for (nm in names(x)) {
    val <- x[[nm]]
    key <- if (is.null(parent)) nm else paste(parent, nm, sep = ".")
    if (is.list(val) && !is.null(names(val))) {
      out <- c(out, flatten_list(val, key))
    } else if (is.atomic(val) && length(val) > 1) {
      vec_names <- paste0(key, "_", seq_along(val))
      out[vec_names] <- as.list(val)
    } else {
      out[[key]] <- if (is.null(val)) NA else val
    }
  }
  out
}
as_chr_id <- function(v) {
  if (is.null(v)) return(NA_character_)
  if (is.list(v) && length(v) == 1L) v <- v[[1]]
  if (is.character(v)) return(v)
  if (is.numeric(v))  return(format(v, scientific = FALSE, trim = TRUE))
  as.character(v)
}
param_rows <- lapply(names(yaml_list), function(id) {
  flat <- flatten_list(yaml_list[[id]])
  id_candidates <- c("H_IDNUM","h_idnum","household_id","cps_hh_id","cps_id","cps_household_id")
  H_IDNUM_val <- NA_character_
  for (cand in id_candidates) if (!is.null(flat[[cand]])) { H_IDNUM_val <- as_chr_id(flat[[cand]]); break }
  flat[["household_id"]] <- NULL
  flat$household_id <- id
  flat$H_IDNUM      <- H_IDNUM_val
  as.data.table(flat)
})
yaml_params_dt <- rbindlist(param_rows, fill = TRUE)
front_cols <- intersect(c("household_id","H_IDNUM"), names(yaml_params_dt))
setcolorder(yaml_params_dt, c(front_cols, setdiff(names(yaml_params_dt), front_cols)))
fwrite(yaml_params_dt, file.path(out_dir, "yaml_inputs.csv"))

# 5) SINGLE-HOUSEHOLD
run_one <- function(hh_id) {
  stopifnot(exists("yaml_list"), !is.null(yaml_list[[hh_id]]), exists("cols"))
  if (!exists(".__INIT_DONE__", envir = .GlobalEnv)) init_env()
  
  # Scalarize single-element lists from YAML (avoid list(0) traps)
  inputs <- lapply(yaml_list[[hh_id]], scalarize)
  
  # Some functions refer to this global
  budget.ALICE <<- inputs$budget.ALICE %||% "survival"
  
  # Base dataframe + ALICE expenses
  df <- function.createData(inputs)
  df$household_id <- hh_id
  df <- BenefitsCalculator.ALICEExpenses(df)
  
  # Switches (default if missing)
  flags <- list(
    TANF      = inputs$APPLY_TANF      %||% TRUE,
    SSI       = inputs$APPLY_SSI       %||% TRUE,
    SSDI      = inputs$APPLY_SSDI      %||% TRUE,
    CHILDCARE = inputs$APPLY_CHILDCARE %||% TRUE,
    HEADSTART = inputs$APPLY_HEADSTART %||% TRUE,
    PREK      = inputs$APPLY_PREK      %||% TRUE,
    CCDF      = inputs$APPLY_CCDF      %||% TRUE,
    FATES     = inputs$APPLY_FATES     %||% FALSE,
    HEALTH    = inputs$APPLY_HEALTHCARE %||% TRUE,
    MED_ADULT = inputs$APPLY_MEDICAID_ADULT %||% TRUE,
    MED_CHILD = inputs$APPLY_MEDICAID_CHILD %||% TRUE,
    ACA       = inputs$APPLY_ACA       %||% TRUE,
    SEC8      = inputs$APPLY_SECTION8  %||% TRUE,
    LIHEAP    = inputs$APPLY_LIHEAP    %||% TRUE,  # try first (CT only below)
    SNAP      = inputs$APPLY_SNAP      %||% TRUE,
    SLP       = inputs$APPLY_SLP       %||% TRUE,
    WIC       = inputs$APPLY_WIC       %||% TRUE,
    RAP       = inputs$APPLY_RAP       %||% FALSE,
    FRSP      = inputs$APPLY_FRSP      %||% FALSE,
    EITC      = inputs$APPLY_EITC      %||% TRUE,
    CTC       = inputs$APPLY_CTC       %||% TRUE,
    CDCTC     = inputs$APPLY_CDCTC     %||% TRUE
  )
  
  # LIHEAP only for CT
  is_ct <- any(na.omit(df$stateAbbrev) == "CT")
  flags$LIHEAP <- flags$LIHEAP && is_ct
  
  # Benefits (order matters)
  df <- BenefitsCalculator.OtherBenefits(df, flags$TANF, flags$SSI, flags$SSDI)
  df <- BenefitsCalculator.Childcare(df, flags$CHILDCARE, flags$HEADSTART, flags$PREK, flags$CCDF, flags$FATES)
  df <- BenefitsCalculator.Healthcare(df, flags$HEALTH, flags$MED_ADULT, flags$MED_CHILD, flags$ACA)
  
  # Food & Housing with LIHEAP-safe fallback
  df <- safe_FoodAndHousing(df, flags)
  
  # Taxes & tax credits
  df <- BenefitsCalculator.TaxesandTaxCredits(df, flags$EITC, flags$CTC, flags$CDCTC)
  
  # Derived variables
  df <- function.createVars(df)
  
  data.table::as.data.table(df)[, ..cols]
}

# 6) SAFE WRAPPER 
safe_run <- function(hh_id) {
  if (!exists(".__INIT_DONE__", envir = .GlobalEnv)) init_env()
  tryCatch(
    run_one(hh_id),
    error = function(e) {
      # Log to file (silent)
      try(write(sprintf("HH %s error: %s", hh_id, e$message),
                file = file.path(out_dir, "error_households.log"), append = TRUE), silent = TRUE)
      NULL
    }
  )
}

# 7) CHUNK RUNNER
chunk_runner <- function(ids, dir_out = out_dir) {
  force(dir_out)  # avoid lazy evaluation issues on workers
  if (!dir.exists(dir_out)) dir.create(dir_out, recursive = TRUE, showWarnings = FALSE)
  
  out_list <- lapply(ids, function(x) suppressWarnings(safe_run(x)))
  ok       <- Filter(Negate(is.null), out_list)
  if (length(ok) == 0L) return(invisible(data.table::data.table()))
  
  dt <- data.table::rbindlist(ok, use.names = TRUE, fill = TRUE)
  fname    <- sprintf("household_%s_%s.csv", ids[1], ids[length(ids)])
  out_file <- file.path(dir_out, fname)
  data.table::fwrite(dt, out_file)
  invisible(dt)
}

# 8) SLURM PLAN
plan(
  batchtools_slurm,
  template  = "slurm.tmpl", 
  resources = list(ncpus = 16, mem = "180G", time = "08:00:00")
)

# 9) BUILD CHUNKS TO SUBMIT ---------------------------------------
job_size <- 50L
all_ids <- hh_ids
full_chks <- split(all_ids, ceiling(seq_along(all_ids) / job_size))
invisible(
  future_lapply(
    X               = full_chks,
    FUN             = chunk_runner,
    future.packages = c("yaml","data.table","compiler"),
    future.globals  = c(
      "chunk_runner","safe_run","run_one","safe_FoodAndHousing",
      "init_env","yaml_list","cols","%||%","scalarize","out_dir"
    )
  )
)


# 10. Combine all household

files <- list.files(
  path       = out_dir,
  pattern    = "^household_household_[0-9]+_household_[0-9]+\\.csv$",
  full.names = TRUE
)

combined_dt <- rbindlist(
  lapply(files, fread, showProgress = TRUE),
  use.names = TRUE,   
  fill      = TRUE
)

output_file <- file.path(out_dir, "all_households.csv")
fwrite(combined_dt, output_file)