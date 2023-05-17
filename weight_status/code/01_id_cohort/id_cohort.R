# ID Cohort for Weight Status Predictions
## Code originally formulated in ~/notebooks/id-cohort/examine_weight_data.Rmd
## Modified to add pregnancy exclusions

# Setup ------------------------------------------------------------------------
renv::activate()

source(here::here("code/common/setup.R"))
# plus additional libraries
library(readxl)
library(arrow)
library(DBI)
library(lubridate)

# DB connections
source(here::here("code/common/connect.R"))

# Diag codes of interest ------------------------------------------------------
# need combination of E66 diag and BMI to qualify for obesity diagnosis 
# (confirmed direction with Dr. <redacted> and references in links provided in this repo)

obese_diags <- c(
  "E66.01", # obese, severe, excess calories
  "E66.09", # obese, excess calories
  "E66.1",  # obese, drug-induced
  "E66.2",  # obese, severe, with breathing problems
  "E66.8",  # obese, other
  "E66.9"   # obese, unspecified
)

ovrwgt_diags <- "E66.3"

obese_bmis   <- paste0("Z68.", c(30:45, 54))

ovrwgt_bmis  <- paste0("Z68.", c(25:29, 53))

normwgt_bmis <- paste0("Z68.", c(20:24, 52))

undrwgt_codes <- c(
  "Z68.51"  # child underweight BMI
)

low_adlt_bmi <- c(
  "Z68.1" # can be normal or under weight BMI, adults
)

# Combine all diag codes that can be used alone, omit those that are "companion" codes
diag_codes <- c(obese_diags, 
                ovrwgt_diags, 
                # obese_bmis, leave these out at this point to not get claims with these codes as the *only* code of interest
                # ovrwgt_bmis, these should be paired with the E66 if E66 is on the claim, so should get these anyway when including E66 codes
                normwgt_bmis, 
                undrwgt_codes, 
                low_adlt_bmi
                # undrwgt_companions
)
  
# Dates of interest ------------------------------------------------------------
end_dt <- as.Date('2022-11-01')
start_dt <- end_dt - lubridate::years(2)

# Connect to tables ------------------------------------------------------------
# members 
membership <- tbl(con_access,
                  in_schema("<redacted>", "<redacted>"))
# birth date info to filter by age
person <- tbl(con_access,
              in_schema("<redacted>", "<redacted>"))
# claims
claims <- tbl(con_access,
              in_schema("<redacted>", "<redacted>")) 

# Pull weight-related claims ---------------------------------------------------
# customer numbers enrolled for 4 of the last 6 months before the "end_dt"
recent <- membership |> 
  left_join(person, by = c("<redacted>" = "<redacted>")) |>
  mutate(tm0  = as.Date(end_dt) - months(0),
         tm1  = as.Date(end_dt) - months(1),
         tm2  = as.Date(end_dt) - months(2),
         tm3  = as.Date(end_dt) - months(3),
         tm4  = as.Date(end_dt) - months(4),
         tm5  = as.Date(end_dt) - months(5)) |>
  mutate(across(starts_with("tm"), ~ case_when(<redacted date> <= . & <redacted date> > . ~ 1, TRUE ~ 0))) |>
  select(<redacted>, starts_with("tm")) |>
  group_by(<redacted>) |>
  summarize(across(starts_with("tm"), max)) |>
  mutate(n_months = tm0 + tm1 + tm2 + tm3 + tm4 + tm5) |>
  ungroup() |>
  filter(n_months >= 4) |>
  select(<redacted>) |>
  compute()

# get all weight-related diagnosis codes from claims
weight_claims <- membership |> # start with membership for the cust-member id crosswalk
  select(<redacted>, <redacted>) |>
  # keep only members who meet enrollment month criteria above
  inner_join(recent, by = "<redacted>") |>
  rename(cust_id = <redacted>) |>
  inner_join(claims, by = c("<redacted>" = "<redacted>")) |>
  # keep the claims that are paid or approved
  filter(clmstat %in% c('PA','AP')) |>
  # keep only the id, date, and diagnoses of interest
  select(cust_id,
         firstdos
         contains("diag")
  ) |>
  # clean up some names
  rename(diagd1 = prdiagcd,
         diag16 = diagn16) |>
  # pivot so that all diags are in one column
  pivot_longer(cols = contains("diag"),
               names_to = "diag_seq",
               names_prefix = "diag",
               values_to = "diag_cd") |>
  # keep only the weight-related diags
  filter(diag_cd %in% local(c(diag_codes,
                              obese_bmis,
                              ovrwgt_bmis))) |>
  compute()

# get most recent weight indicator(s) for each person during time period
most_recent_weight <- weight_claims |>
  # keep only those within the time period of interest
  filter(between(firstdos, local(start_dt), local(end_dt))) |>
  # keep everything for the most recent date for each person
  group_by(cust_id) |>
  window_order(desc(firstdos)) |>
  mutate(r = min_rank()) |> # rank with ties all equaling 1
  ungroup() |>
  filter(r == 1) |>
  # make indicators for each type of diagnosis
  mutate(obese_e66   = ifelse(diag_cd %in% local(obese_diags),   1, 0),
         ovrwgt_e66  = ifelse(diag_cd %in% local(ovrwgt_diags),  1, 0),
         obese_bmi   = ifelse(diag_cd %in% local(obese_bmis),    1, 0),
         ovrwgt_bmi  = ifelse(diag_cd %in% local(ovrwgt_bmis),   1, 0),
         normwgt_bmi = ifelse(diag_cd %in% local(c(normwgt_bmis, low_adlt_bmi)), 1, 0),
         undrwgt_bmi = ifelse(diag_cd %in% local(undrwgt_codes), 1, 0)) |>
  # get a single row for each customer id, bring date along with it
  group_by(cust_id, firstdos) |>
  summarize(across(c(obese_e66,
                     ovrwgt_e66,
                     obese_bmi,
                     ovrwgt_bmi,
                     normwgt_bmi,
                     undrwgt_bmi), max)) |>
  ungroup() |>
  rename(index_dt = firstdos) |>
  compute()

# get diags from claims in the prior year
weight_claims_prior_yr <- most_recent_weight |>
  # start with the most recent weight-related claims data 
  # and keep just the customer id and date
  distinct(cust_id, index_dt) |>
  #j oin all weight-related claims
  left_join(weight_claims, by = "cust_id") |>
  # keep only claims in the 364 days before the index date
  filter(between(firstdos, index_dt - days(365), index_dt - days(1))) |>
  # make indicators for each type of diagnosis in prior year
  mutate(prior_obese_e66   = ifelse(diag_cd %in% local(obese_diags),   1, 0),
         prior_ovrwgt_e66  = ifelse(diag_cd %in% local(ovrwgt_diags),  1, 0),
         prior_obese_bmi   = ifelse(diag_cd %in% local(obese_bmis),    1, 0),
         prior_ovrwgt_bmi  = ifelse(diag_cd %in% local(ovrwgt_bmis),   1, 0),
         prior_normwgt_bmi = ifelse(diag_cd %in% local(c(normwgt_bmis, low_adlt_bmi)), 1, 0),
         prior_undrwgt_bmi = ifelse(diag_cd %in% local(undrwgt_codes), 1, 0)) |>
  # get a single row for each customer id
  group_by(cust_id) |>
  summarize(across(c(prior_obese_e66,
                     prior_ovrwgt_e66,
                     prior_obese_bmi,
                     prior_ovrwgt_bmi,
                     prior_normwgt_bmi,
                     prior_undrwgt_bmi), max)) |>
  ungroup() |>
  compute()

# Make table before exclusions -------------------------------------------------
weight_df <- most_recent_weight |> # start with most recent weight indicators
  # add prior weight indicators
  left_join(weight_claims_prior_yr, by = "cust_id") |>
  # add some basic demographics
  left_join(ca_person, by = c("cust_id" = "<redacted>")) |>
  select(cust_id:prior_undrwgt_bmi,
         birth_dt) |>
  # pull down from server
  collect() |>
  mutate(age = floor(as.integer(index_dt - birth_dt) / 365.25)) |>
  select(-birth_dt) |>
  # fill NAs in prior year claims with 0
  mutate(across(starts_with("prior"), ~ ifelse(is.na(.), 0, .))) |>
  # simplify "obese" to mean overweight or obese
  # and "not obese" to mean normal weight or underweight
  # exclude those without confirmed weight status
  mutate(obese =
           case_when(obese_e66   == 1 & obese_bmi        == 1 ~ 1,
                     obese_e66   == 1 & prior_obese_bmi  == 1 ~ 1,
                     obese_bmi   == 1 & prior_obese_e66  == 1 ~ 1,
                     ovrwgt_e66  == 1 & ovrwgt_bmi       == 1 ~ 1,
                     ovrwgt_e66  == 1 & prior_ovrwgt_bmi == 1 ~ 1,
                     ovrwgt_bmi  == 1 & prior_ovrwgt_e66 == 1 ~ 1,
                     normwgt_bmi == 1 & 
                       obese_e66        == 0 &
                       ovrwgt_e66       == 0 &
                       obese_bmi        == 0 &
                       ovrwgt_bmi       == 0 &
                       prior_obese_e66  == 0 & 
                       prior_obese_bmi  == 0 & 
                       prior_ovrwgt_e66 == 0 &
                       prior_ovrwgt_bmi == 0             ~ 0,
                     undrwgt_bmi == 1 &
                       obese_e66        == 0 &
                       ovrwgt_e66       == 0 &
                       obese_bmi        == 0 &
                       ovrwgt_bmi       == 0 &
                       prior_obese_e66  == 0 & 
                       prior_obese_bmi  == 0 & 
                       prior_ovrwgt_e66 == 0 &
                       prior_ovrwgt_bmi == 0             ~ 0,
                     obese_e66 == 1  & ovrwgt_bmi == 1   ~ 1,
                     ovrwgt_e66 == 1 & obese_bmi == 1    ~ 1)) |>
  # don't include members under age 2
  filter(between(age, 2, 110),
         !is.na(obese)) |>
  # keep only a few important fields
  select(cust_id, 
         index_dt,
         obese)

# Add members identified with HIE text data ------------------------------------
hie_bmi <- s3read_using(FUN = arrow::read_parquet,
                        bucket = "<redacted>",
                        object = "<redacted>/data/dev/incoming/bmi_updated.parquet")

## Consolidate hie data
# keep records in the past 2 years,
# remove records with obese label conflicts,
# remove records that already appear in the weight status data,
# keep the most recent record for each customer,
# and make obese indicator
hie_bmi2 <- hie_bmi |>
  rename(cust_id = <redacted>) |>
  mutate(index_dt = as.Date(<redacted>),
         # make indicators for which values the member has (bmi value or percentile)
         has_val = case_when(is.na(bmi_val) &
                               is.na(bmi_range_lower) &
                               is.na(bmi_range_upper) ~ 0,
                             TRUE                     ~ 1),
         has_pct = case_when(is.na(bmi_pct) &
                               is.na(bmi_pct_range_lower) &
                               is.na(bmi_pct_range_upper) ~ 0,
                             TRUE                         ~ 1),
         # assign obese label based on value and based on percentile
         obese_val = case_when(bmi_val >= 25 ~ 1,
                               bmi_val <  25 ~ 0,
                               bmi_range_lower >= 25 ~ 1,
                               bmi_range_upper <  25 ~ 0),
         obese_pct = case_when(bmi_pct >= 85 ~ 1,
                               bmi_pct <  85 ~ 0,
                               bmi_pct_range_lower >= 85 ~ 1,
                               bmi_pct_range_lower ==  5 ~ 0),
         # make final label that gets rid of conflicts within the same record
         obese = case_when(has_val == 1 & has_pct == 1 & obese_val == obese_pct ~ obese_val,
                           has_val == 1 & has_pct == 0                          ~ obese_val,
                           has_val == 0 & has_pct == 1                          ~ obese_pct,
                           E66 == TRUE                                          ~ 1)) |>
  filter(between(index_dt, start_dt, end_dt),
         !(cust_id %in% weight_df$cust_id),
         !is.na(obese)) |>
  # keep their most recent record
  group_by(cust_id) |>
  mutate(max_index_dt = max(index_dt, na.rm = TRUE)) |>
  ungroup() |>
  filter(index_dt == max_index_dt) |>
  distinct(cust_id, index_dt, obese)

# find dupes (where most recent record is unclear on label)
dupes <- hie_bmi2 |>
  group_by(cust_id) |>
  summarize(n = n()) |>
  filter(n > 1) |>
  pull(cust_id)

# remove the customers with dupes
hie_bmi3 <- hie_bmi2 |>
  filter(!cust_id %in% dupes)

# Add HIE data to claims-based data --------------------------------------------
weight_df2 <- bind_rows(weight_df, hie_bmi3)

# Exclude members during pregnancy ---------------------------------------------
# make function to read in pregnancy and delivery codes and collapse into a string for SQL
pcode_str <- function(sheet_nm, column_nm) {
  vec <- 
    read_excel(path = here("docs/ref-codes/preg_inclusion_exclusion.xlsx"),
               sheet = sheet_nm,
               col_types = "text") |>
    pull(column_nm)
}

# read in codes
preg_incl <- pcode_str("inclusion_icd",
                       "diagnosis_code")
preg_excl <- pcode_str("exclusion_icd",
                       "diagnosis_code")
deliv_icd <- pcode_str("delivery_icd",
                       "diagnosis_code")
deliv_drg <- pcode_str("delivery_drg",
                       "drg")

# get claims data with preg and delivery diagnosis codes
preg_deliv_diag <- membership |> # start with ca_membership for the cust-member id crosswalk
  select(<redacted>, <redacted>) |>
  # keep only members who meet enrollment month criteria above
  inner_join(recent, by = "<redacted>") |>
  rename(cust_id = <redacted>) |>
  inner_join(claims, by = c("<redacted>" = "<redacted>")) |>
  # filter claims on age and sex
  filter(sexcod == "F",
         between(ageyears, 8, 50)) |>
  # keep only the id, date, and diagnoses of interest
  select(cust_id,
         firstdos,
         contains("diag")
  ) |>
  # pivot so that all diags are in one column
  pivot_longer(cols = contains("diag"),
               names_to = "diag_seq",
               names_prefix = "diag",
               values_to = "diag_cd") |>
  # keep only the diags of interest
  filter(diag_cd %in% local(c(preg_incl,
                              preg_excl,
                              deliv_icd))) |>
  # make inclusion/exclusion indicators
  mutate(p_incl = case_when(diag_cd %in% local(preg_incl) ~ 1, TRUE ~ 0),
         p_excl = case_when(diag_cd %in% local(preg_excl) ~ 1, TRUE ~ 0),
         d_incl = case_when(diag_cd %in% local(deliv_icd) ~ 1, TRUE ~ 0)) |>
  # for each customer and claim date, aggregate inclu/exclu indicators
  group_by(cust_id, firstdos) |>
  summarize(across(c(p_incl, p_excl, d_incl), max)) |> 
  ungroup() |>
  # for each date, assign deliv or preg; take the deliv indicator if both exist
  mutate(assignment = case_when(d_incl == 1               ~ "d",
                                p_incl == 1 & p_excl == 0 ~ "p")) |>
  # keep only valid assignments
  filter(!is.na(assignment)) |>
  select(cust_id, firstdos, assignment) |>
  compute()

# get claims data with delivery drg codes
deliv_drg_tbl <- membership |> # start with membership for the cust-member id crosswalk
  select(<redacted>, <redacted>) |>
  # keep only members who meet enrollment month criteria above
  inner_join(recent, by = "<redacted>") |>
  rename(cust_id = <redacted>) |>
  inner_join(cdw_claims, by = c("<redacted>" = "<redacted>")) |>
  # filter claims on age and sex
  filter(sexcod == "F",
         between(ageyears, 8, 50)) |>
  # keep only the id, date, drg columns
  select(cust_id,
         firstdos,
         drgcod,
         subdrg) |>
  # keep only the delivery drgs
  filter(drgcod %in% local(deliv_drg) | subdrg %in% local(deliv_drg)) |>
  # remove duplicates
  distinct(cust_id, firstdos) |>
  compute() 

# make separate tables with pregs and deliveries
pregs <-  preg_deliv_diag |> 
  filter(assignment == "p") |>
  rename(preg_clm_dt = firstdos) |>
  # load into memory and make exclusion date range
  collect() |>
  mutate(preg_exclude_start = lubridate::add_with_rollback(preg_clm_dt, months(-9)),
         preg_exclude_end   = lubridate::add_with_rollback(preg_clm_dt, months(12))) 

delivs <- preg_deliv_diag |>
  filter(assignment == "d") |>
  full_join(deliv_drg_tbl, by = c("cust_id", "firstdos")) |>
  rename(deliv_clm_dt = firstdos) |>
  distinct(cust_id, deliv_clm_dt) |>
  collect() |>
  # load into memory and make exclusion date range
  mutate(deliv_exclude_start = lubridate::add_with_rollback(deliv_clm_dt, months(-9)),
         deliv_exclude_end   = lubridate::add_with_rollback(deliv_clm_dt, months(6)))

# make df of pregnancy rows to remove (cust id AND date)
rm_preg_dates <- pregs |>
  inner_join(delivs, by = "cust_id", multiple = "all") |>
  # keep those preganacy claim dates that are within the delivery-based date window
  filter(between(preg_clm_dt, deliv_exclude_start, deliv_exclude_end)) |>
  distinct(cust_id, preg_clm_dt)

# remove excess pregnancy rows, and prepare df for row bind with deliveries
pregs2 <- pregs |>
  anti_join(rm_preg_dates, by = c("cust_id","preg_clm_dt")) |>
  rename(exclude_start = preg_exclude_start,
         exclude_end   = preg_exclude_end) |>
  select(cust_id, exclude_start, exclude_end)

# prepare df for row bind with pregnancies
delivs2 <- delivs |>
  rename(exclude_start = deliv_exclude_start,
         exclude_end   = deliv_exclude_end) |>
  select(cust_id, exclude_start, exclude_end)

# make df of weight/obese rows to exclude (with cust_id AND date)
preg_exclusions <- bind_rows(pregs2, delivs2) |>
  inner_join(weight_df2, by = "cust_id", multiple = "all") |>
  filter(between(index_dt, exclude_start, exclude_end)) |>
  distinct(cust_id, index_dt)

# Make final table -------------------------------------------------------------
# start with the weight data and exclude the rows during pregnancies
final <- weight_df2 |>
  anti_join(preg_exclusions, by = c("cust_id", "index_dt"))

# Write table to S3 ------------------------------------------------------------
s3write_using(x = final,
              FUN = write.csv,
              row.names = FALSE
              bucket = "<redacted>",
              object = "<redacted>/data/dev/target/full_cohort_and_labels.csv"
              )

# Finishing Steps --------------------------------------------------------------
DBI::dbDisconnect(con_access)
renv::snapshot()
