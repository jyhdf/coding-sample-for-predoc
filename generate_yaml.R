install.packages(c("readr","dplyr","tidyr","stringr","yaml","glue","fs","purrr","data.table"))
library(readr)
library(dplyr)
library(tidyr)
library(stringr)
library(yaml)
library(glue)
library(fs)
library(purrr)
library(data.table)

setwd('/Users/wangyuexin/Desktop/RA for Jim/100PercentTax')

# 1. Read both files with explicit col_types to keep IDs as character:
hh <- read_csv(
  "/Users/wangyuexin/Desktop/RA for Jim/CPS/asecpub24csv/cps_geo_v4/hhpub24_with_imputed_geography_fixed.csv",
  col_types = cols(
    H_IDNUM = col_character(),
    .default = col_guess()
  )
)

pp <- read_csv(
  "/Users/wangyuexin/Desktop/RA for Jim/CPS/asecpub24csv/pppub24.csv",
  col_types = cols(
    PERIDNUM  = col_character(),
    A_LINENO  = col_integer(),
    A_AGE      = col_integer(),
    DIS_YN     = col_integer(),
    PEDISEYE   = col_integer(),
    DSAB_VAL   = col_integer(),
    A_FAMREL   = col_integer(),
    A_MARITL   = col_integer(),
    .default   = col_guess()
  )
)
# 1.a.  Household / Filter out invalid households

hh_clean <- hh %>%
  filter(
    H_TYPEBC == 0,                     # Keep only Type A households 
    HRHTYPE   %in% 1:8,                # Keep only residential household types 01–08
    H_NUMPER > 0,                      # Exclude households with zero members
    H_NUMPER  <= 12,                   # Drop households with more than 12 persons
    HHSTATUS != 0,                     # Exclude households marked invalid
    HEFAMINC != -1,                    # Keep households within the income screener universe
    H_RESPNM != 0                      # Keep only households with a valid respondent
  )

# 1.b. Create pp_clean2: only persons in hh_clean & Sequence number ≤12 
pp_clean <- pp %>%
  filter(
    substr(PERIDNUM, 1, 20) %in% hh_clean$H_IDNUM,  
    P_SEQ  <= 12 
  ) %>%
  mutate(
    H_IDNUM = substr(PERIDNUM, 1, 20)             
  )


# 1.c Ensure adults come first
pp_clean <- pp_clean %>%
  group_by(H_IDNUM) %>%
  arrange(
    desc(A_AGE >= 18),  # adults first
    P_SEQ               # then original order
    
  ) %>%
  mutate(
    P_SEQ_NEW = row_number()  # reassign sequence within each household
  ) %>%
  ungroup()

# 1.d Append two‐letter state abbreviations to final_location
state_map <- tibble(
  state_full = c(state.name, "District of Columbia"),
  state_abbr = c(state.abb, "DC")
)
normalize_ct_town <- function(place_vec) {
  base <- stringr::str_trim(
    stringr::str_replace(place_vec, "\\s+(city|town)\\s*$", "")
  )
  paste0(base, " town")
}

hh_clean2 <- hh_clean %>%
  left_join(state_map, by = c("final_state_name" = "state_full")) %>%
  mutate(
    final_location = if_else(
      state_abbr == "CT",
      stringr::str_c(normalize_ct_town(final_location), ", ", state_abbr),
      stringr::str_c(stringr::str_trim(final_location), ", ", state_abbr)
    )
  ) %>%
  select(-state_abbr)



# 2. Build the person‐wide table (up to 12 persons per household) using A_LINENO

persons_wide <- pp_clean %>%
  transmute(
    H_IDNUM,
    p_seq    = P_SEQ_NEW,
    age      = A_AGE,
    disabled = as.integer(DIS_YN == 1),
    blind    = as.integer(PEDISEYE == 1),
    ssdi     = DSAB_VAL
  ) %>%
  pivot_wider(
    id_cols     = H_IDNUM,
    names_from  = p_seq,
    values_from = c(age, disabled, blind, ssdi),
    names_glue  = "{.value}{p_seq}"
  )


# 3. Married flag from reference person - if multiple reference people in household, default to married if any of them are married
married_flag <- pp_clean %>%
  filter(A_FAMREL == 1) %>%
  transmute(H_IDNUM, married = as.integer(A_MARITL == 1)) %>%
  distinct()
married_flag <- data.table(married_flag)
setorder(married_flag, H_IDNUM, -married)
married_flag <- married_flag[,.SD[1],keyby=H_IDNUM]


# 4. Previous-SSI flag from household
prev_ssi_flag <- hh %>% transmute(H_IDNUM, prev_ssi = HSSI_YN)

# 5. Join everything back onto hh
hh_full <- hh_clean2 %>%
  left_join(persons_wide,   by = "H_IDNUM") %>%
  left_join(married_flag,   by = "H_IDNUM") %>%
  left_join(prev_ssi_flag,  by = "H_IDNUM")
hh_full<-data.table(hh_full)
hh_full[,married := ifelse(is.na(married),0,married)]
hh_full <- hh_full %>%
  mutate(prev_ssi = as.integer(HSSI_YN == 1))


# 6. Create configs folder
setwd('/Users/wangyuexin/Desktop/RA for Jim/100PercentTax')
dir_create("configs")

# 7. Static fields
static_fields <- list(
  ruleYear               = list(2025L),
  Year                   = list(2025L),
  income_start           = 0L,
  income_end             = 500000L,
  income_increase_by     = 1000L,
  income.investment      = list(0L),
  income.gift            = list(0L),
  income.child_support   = list(0L),
  APPLY_CHILDCARE        = TRUE,
  APPLY_CCDF             = TRUE,
  APPLY_HEALTHCARE       = TRUE,
  APPLY_TANF             = TRUE,
  APPLY_HEADSTART        = TRUE,
  APPLY_PREK             = TRUE,
  APPLY_LIHEAP           = TRUE,
  APPLY_MEDICAID_ADULT   = TRUE,
  APPLY_MEDICAID_CHILD   = TRUE,
  APPLY_ACA              = TRUE,
  APPLY_SECTION8         = TRUE,
  APPLY_RAP              = FALSE,
  APPLY_FRSP             = FALSE,
  APPLY_SNAP             = TRUE,
  APPLY_SLP              = TRUE,
  APPLY_WIC              = TRUE,
  APPLY_EITC             = TRUE,
  APPLY_TAXES            = TRUE,
  APPLY_CTC              = TRUE,
  APPLY_CDCTC            = TRUE,
  APPLY_FATES            = TRUE,
  APPLY_SSI              = TRUE,
  APPLY_SSDI             = TRUE,
  k_ftorpt               = "FT",
  schoolagesummercare    = "PT",
  headstart_ftorpt       = "PT",
  preK_ftorpt            = "PT",
  contelig.headstart     = FALSE,
  contelig.earlyheadstart= FALSE,
  contelig.ccdf          = FALSE,
  budget.ALICE           = "survivalforcliff",
  assets.car1            = 0L
)

# 8. Write one YAML file per household
walk2(hh_full$row_id, split(hh_full, hh_full$row_id), ~{
  id  <- .x
  row <- .y
  
  dyn <- list(
    household_id    = row$H_IDNUM,
    
    # Ages
    agePerson1      = list(row$age1),   agePerson2  = list(row$age2),
    agePerson3      = list(row$age3),   agePerson4  = list(row$age4),
    agePerson5      = list(row$age5),   agePerson6  = list(row$age6),
    agePerson7      = list(row$age7),   agePerson8  = list(row$age8),
    agePerson9      = list(row$age9),   agePerson10 = list(row$age10),
    agePerson11     = list(row$age11),  agePerson12 = list(row$age12),
    
    married         = list(row$married),
    prev_ssi        = list(row$prev_ssi),
    
    # Disabilities
    disability1     = list(row$disabled1),  disability2 = list(row$disabled2),
    disability3     = list(row$disabled3),  disability4 = list(row$disabled4),
    disability5     = list(row$disabled5),  disability6 = list(row$disabled6),
    disability7     = list(row$disabled7),  disability8 = list(row$disabled8),
    disability9     = list(row$disabled9),  disability10=list(row$disabled10),
    disability11    = list(row$disabled11), disability12=list(row$disabled12),
    
    # Blindness
    blind1          = list(row$blind1),  blind2 = list(row$blind2),
    blind3          = list(row$blind3),  blind4 = list(row$blind4),
    blind5          = list(row$blind5),  blind6 = list(row$blind6),
    
    # SSDI
    ssdiPIA1        = list(row$ssdi1),   ssdiPIA2 = list(row$ssdi2),
    ssdiPIA3        = list(row$ssdi3),   ssdiPIA4 = list(row$ssdi4),
    ssdiPIA5        = list(row$ssdi5),   ssdiPIA6 = list(row$ssdi6),
    
    # Geography & flags
    locations       = list(row$final_location),
    empl_healthcare = list(row$NOW_HCOV),
    ownorrent       = list(if (row$H_TENURE == 1) "own" else "rent"),
    `assets.cash`   = list(row$HFINVAL),
    `disab.work.exp`= list(row$HDISVAL)
  )
  
  cfg     <- c(static_fields, dyn)
  yml_txt <- as.yaml(cfg)
  write_file(yml_txt, file = glue("configs/household_{id}.yml"))
})
