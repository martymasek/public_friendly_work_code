# Identify the cohort for the Aging In Place Study
# Following latest logic map and incorporating previous iltc logic
# Includes code for counting the contiguous stay months and 
## counting SNF months when they occur after other institutional stays,
## but not counting SNF months that are at the beginning of institutional stays (following Yun et al 2010)

# Use claims to categorize members ages 55+ into long term care (LTC) status:
## 1. long institutional LTC stays (4+ months)
## 2. short institutional LTC stays (<4 months)
## 3. home and community-based services HCBS (no institutional stays)
## 4. no LTC

# Setup ------------------------------------------------------------------------
library(DBI)
library(tidyverse)
library(dbplyr)
library(odbc)

options(scipen = 999)

# load DB connection credentials
dbIni <- ini::read.ini(here::here("ini/db.ini"))

# connect to AW
con_access <- dbConnect(odbc::odbc(),
                        Driver   = dbIni$aw_access$odbcDriver,
                        Server   = dbIni$aw_access$odbcServer,
                        Database = dbIni$aw_access$dbName,
                        username = dbIni$aw_access$un,
                        password = dbIni$aw_access$pw,
                        bigint   = "integer"
)

# Codes of interest in claims data ---------------------------------------------
inst_pos <- c('31', # SNF
              '32', # NF
              '33', # CC
              '13'  # ALF
)
inst_proc <- c('99301','99302', '99303','99311','99312','99313','99315','99316','99379','99380','G0066')

# adult day care codes to ignore these claims. References: https://hcpcs.codes/search/?q=S510 and https://www.health.ny.gov/facilities/long_term_care/reimbursement/letters/dal_2017-09-22_billing_codes.htm
day_proc <- c('55100', 'S5100', 'S5101', 'S5102', 'S5103', 'S5105',          # day care
              'A0080', 'T2001', 'T2002', 'T2003', 'T2004', 'T2005', 'T2007') # transportation

# Home health service codes from https://www.health.ny.gov/facilities/long_term_care/reimbursement/letters/dal_2017-09-22_billing_codes.htm
home_proc <- c("S5130", # personal care aide level I, housekeeper
               
               "T1019", # personal care aide level II, personal care services
               "T1020", # personal care aide level II, live-in
               "S5125", # home health aid
               "S5126", # home health aid, live-in
               "S9122", # home health aid, advanced
               
               "T1001", # Nursing Assessment In-Home Initial Evaluation
               # "T2024", # Nursing Assessment (does not specify in-home)
               "T1030", # Nursing care, in the home, by registered nurse, per diem
               "S9123", # Nursing care, in the home, by registered nurse, per hour
               "T1002", # Nursing care, in the home, by registered nurse, per 15 min.
               "T1031", # Nursing care, in the home, by licensed practical nurse, per diem
               "S9124", # Nursing Care, in the home; by licensed practical nurse, per hour
               "T1003", # LPN/LVN services, up to 15 minutes
               
               "S9129", # Occupational therapy, in the home, per diem
               "S9131", # Physical therapy, in the home, per diem
               "S9128", # Speech therapy, in the home, per diem
               # "G0237", # Respiratory Therapy (does not specify in-home)
               # "G0238", # Respiratory Therapy (does not specify in-home)
               # "S9470", # Nutritional Counseling	Per visit (does not specify in-home)
               "S9127", # Social work visit, in the home, per diem
               # "T1013", # Sign Language/Oral interpreter (doe not specify in-home)
               "S5165", # Home modifications; per service
               "T1028"  # Assessment of home, physical and family environment
               
               # "55102", # Day care services, adult; per diem	(this is community-based)*
)

#* From NYSDOH on Adult Day Care: All patients are required to be eligible for nursing home placement and have a physician's order for the day services. Most services are operated by nursing homes, but are not necessarily located at the nursing homes. Generally, services are offered from one to five days per week, with some services available on weekends.
#  From va.gov on Adult Day Care: Adult Day Health Care is a program Veterans can go to during the day for social activities, peer support, companionship, and recreation. The program is for Veterans who need help with activities of daily living. Examples include help with bathing, dressing, or fixing meals.


# Barbara T. says "All home health claims can be identified" with this revenue code
home_rev <- c("023") # this is the HIPPS for Home Health (0023). Follow-up: What about revenue codes: 0560-0590?



# Pull data ====================================================================
## ODS Claims Tables (header and detail) ---------------------------------------
c_header <- 
  tbl(con_access,
      in_schema("<redacted>","claim_header")) |>
  select(claim_header_pk,
         place_of_service_cd,
         claim_service_from_dt,
         claim_service_to_dt,
         admission_dt,
         discharge_dt) |>
  filter(claim_service_from_dt <= '2019-12-31', # get claims for events that started between 2017 and 2019, inclusive
         claim_service_from_dt >= '2017-01-01')
c_detail <- 
  tbl(con_access,
      in_schema("<redacted>","claim_ln_detail")) |>
  select(claim_header_fk,
         person_id,
         patient_age_num,
         place_of_service_cd,
         cpt_cd,
         service_cd,
         medical_procedure_cd_fk,
         revenue_cd,
         medical_revenue_cd_fk,
         claim_ln_service_from_dt,
         claim_ln_service_to_dt
  ) |>
  rename(claim_ln_place_of_service_cd = place_of_service_cd)

proc_ref <- 
  tbl(con_access,
      in_schema("<redacted>", "medical_procedure_cd")) |>
  filter(procedural_coding_system_cd == "CP") |>
  select(medical_procedure_cd_pk,
         procedure_cd)

rev_ref <- 
  tbl(con_access,
      in_schema("<redacted>", "medical_revenue_cd")) %>%
  select(medical_revenue_cd_pk,
         revenue_cd) %>%
  rename(revenue_cd2 = revenue_cd)


# Join and filter to instit. LTC claims ages 55+
claims <- c_header |>
  left_join(c_detail, by = c("claim_header_pk"         = "claim_header_fk")) |>
  left_join(proc_ref, by = c("medical_procedure_cd_fk" = "medical_procedure_cd_pk")) |>
  left_join(rev_ref,  by = c("medical_revenue_cd_fk"   = "medical_revenue_cd_pk")) 
  

## Membership and Demog --------------------------------------------------------
# Those with membership in 2017-2019, inclusive
m <- tbl(con_access,
         in_schema("<redacted>","<redacted>")) |>
  filter(<redacted> >= '2017-01-01',      # ignore memberships that expired before 2017
         <redacted> <= '2019-12-31',      # ignore memberships that started after  2019
         <redacted> >= '2017-01-01',      # same as above, but with benefit packages
         <redacted> <= '2019-12-31'
         )

# People ages 55+
p <- tbl(con_access,
         in_schema("<redacted>", "<redacted>")) |>
  filter(birth_dt <= '1964-01-01') # must be 55+ at the beginning of 2019)

members <- m |>
  inner_join(p, by = c("<redacted>" = "<redacted>"))


# Define Groups ================================================================
## Enrolled in 2019 ages 55+ ---------------------------------------------------
members_19 <- members |>
  filter(<redacted> >= '2019-01-01' ,      # ignore memberships that expired before 2019
         <redacted> <= '2019-12-31' ,      # ignore memberships that started after  2019
         <redacted> >= '2019-01-01',       # same as above, but with benefit packages
         <redacted> <= '2019-12-31')

members_19 |>
  distinct(<redacted>) |>
  count() |>
  pull()

## Continuous Enrollment -------------------------------------------------------
### 2017-2018, inclusive, define months of interest
months_1718 <- paste0(c(rep("2017-", 12), rep("2018-", 12)),
                 rep(c("01","02","03","04","05","06","07","08","09","10","11","12"), 2),
                 rep("-01", 24))

# continuous enrollment 2017-2018, inclusive
enrollment_months <- members |>
  distinct(<redacted>,
           <redacted>,
           <redacted>) |>
  mutate(
    # make member month enrollment... combine_intervals and across methods are being buggy, so do this manual way
    mm_1701 = case_when(between(local(months_1718[1]),  <redacted>, <redacted>)  ~ 1, TRUE ~ 0),
    mm_1702 = case_when(between(local(months_1718[2]),  <redacted>, <redacted>)  ~ 1, TRUE ~ 0),
    mm_1703 = case_when(between(local(months_1718[3]),  <redacted>, <redacted>)  ~ 1, TRUE ~ 0),
    mm_1704 = case_when(between(local(months_1718[4]),  <redacted>, <redacted>)  ~ 1, TRUE ~ 0),
    mm_1705 = case_when(between(local(months_1718[5]),  <redacted>, <redacted>)  ~ 1, TRUE ~ 0),
    mm_1706 = case_when(between(local(months_1718[6]),  <redacted>, <redacted>)  ~ 1, TRUE ~ 0),
    mm_1707 = case_when(between(local(months_1718[7]),  <redacted>, <redacted>)  ~ 1, TRUE ~ 0),
    mm_1708 = case_when(between(local(months_1718[8]),  <redacted>, <redacted>)  ~ 1, TRUE ~ 0),
    mm_1709 = case_when(between(local(months_1718[9]),  <redacted>, <redacted>)  ~ 1, TRUE ~ 0),
    mm_1710 = case_when(between(local(months_1718[10]), <redacted>, <redacted>)  ~ 1, TRUE ~ 0),
    mm_1711 = case_when(between(local(months_1718[11]), <redacted>, <redacted>)  ~ 1, TRUE ~ 0),
    mm_1712 = case_when(between(local(months_1718[12]), <redacted>, <redacted>)  ~ 1, TRUE ~ 0),
    mm_1801 = case_when(between(local(months_1718[13]), <redacted>, <redacted>)  ~ 1, TRUE ~ 0),
    mm_1802 = case_when(between(local(months_1718[14]), <redacted>, <redacted>)  ~ 1, TRUE ~ 0),
    mm_1803 = case_when(between(local(months_1718[15]), <redacted>, <redacted>)  ~ 1, TRUE ~ 0),
    mm_1804 = case_when(between(local(months_1718[16]), <redacted>, <redacted>)  ~ 1, TRUE ~ 0),
    mm_1805 = case_when(between(local(months_1718[17]), <redacted>, <redacted>)  ~ 1, TRUE ~ 0),
    mm_1806 = case_when(between(local(months_1718[18]), <redacted>, <redacted>)  ~ 1, TRUE ~ 0),
    mm_1807 = case_when(between(local(months_1718[19]), <redacted>, <redacted>)  ~ 1, TRUE ~ 0),
    mm_1808 = case_when(between(local(months_1718[20]), <redacted>, <redacted>)  ~ 1, TRUE ~ 0),
    mm_1809 = case_when(between(local(months_1718[21]), <redacted>, <redacted>)  ~ 1, TRUE ~ 0),
    mm_1810 = case_when(between(local(months_1718[22]), <redacted>, <redacted>)  ~ 1, TRUE ~ 0),
    mm_1811 = case_when(between(local(months_1718[23]), <redacted>, <redacted>)  ~ 1, TRUE ~ 0),
    mm_1812 = case_when(between(local(months_1718[24]), <redacted>, <redacted>)  ~ 1, TRUE ~ 0)
    ) |>
  select(<redacted>,
         starts_with("mm_")) |>
  collect() |>
  group_by(<redacted>) |>
  summarize_at(vars(starts_with('mm')), max) |>
  mutate(months_enrolled = rowSums(across(starts_with('mm')))) |>
  select(<redacted>, 
         months_enrolled)

# summary(enrollment_months$months_enrolled)
length(setdiff(pull(select(members_19, <redacted>)), enrollment_months$<redacted>))

## Utilization in 2019 ---------------------------------------------------------
utiliz_19 <- members |>
  inner_join(claims, by = c("<redacted>" = "<redacted>")) |>
  filter(claim_service_from_dt <= '2019-12-31', 
         claim_service_from_dt >= '2019-01-01') |>
  distinct(<redacted>)

## Utilization in 2017-2018 ----------------------------------------------------
utiliz_1718 <- members |>
  inner_join(claims, by = c("<redacted>" = "<redacted>")) |>
  filter(claim_service_from_dt <= '2018-12-31', 
         claim_service_from_dt >= '2017-01-01') |>
  distinct(<redacted>)

## ILTC (non-SNF) in 2019 ------------------------------------------------------
iltc_19 <- members |>
  inner_join(claims, by = c("<redacted>" = "<redacted>")) |>
  filter(claim_service_from_dt <= '2019-12-31', 
         claim_service_from_dt >= '2019-01-01') |>
  # place of service: 32 NH, 33 CC, 13 ALF
  filter(place_of_service_cd %in% c('32','33','13') | 
           claim_ln_place_of_service_cd %in% c('32','33','13')) |>
  # remove lines with adult day care service code
  filter(!(cpt_cd       %in% day_proc),
         !(service_cd   %in% day_proc),
         !(procedure_cd %in% day_proc)) |>
  distinct(<redacted>)

## ILTC (non-SNF) in 2017-2018 -------------------------------------------------
iltc_1718 <- members |>
  inner_join(claims, by = c("<redacted>" = "<redacted>")) |>
  filter(claim_service_from_dt <= '2018-12-31', 
         claim_service_from_dt >= '2017-01-01') |>
  # place of service: 32 NH, 33 CC, 13 ALF
  filter(place_of_service_cd %in% c('32','33','13') | 
           claim_ln_place_of_service_cd %in% c('32','33','13')) |>
  # remove lines with adult day care service code
  filter(!(cpt_cd       %in% day_proc),
         !(service_cd   %in% day_proc),
         !(procedure_cd %in% day_proc)) |>
  distinct(<redacted>)

## SNF in 2019 -----------------------------------------------------------------
snf_19 <- members |>
  inner_join(claims, by = c("<redacted>" = "<redacted>")) |>
  filter(claim_service_from_dt <= '2019-12-31', 
         claim_service_from_dt >= '2019-01-01') |>
  # place of service: 31 SNF
  filter(place_of_service_cd == '31' | claim_ln_place_of_service_cd == '31') |>
  # remove lines with adult day care service code
  filter(!(cpt_cd       %in% day_proc),
         !(service_cd   %in% day_proc),
         !(procedure_cd %in% day_proc)) |>
  distinct(<redacted>)

## SNF in 2017-2018 ------------------------------------------------------------
snf_1718 <- members |>
  inner_join(claims, by = c("<redacted>" = "<redacted>")) |>
  filter(claim_service_from_dt <= '2018-12-31', 
         claim_service_from_dt >= '2017-01-01') |>
  # place of service: 31 SNF
  filter(place_of_service_cd == '31' | claim_ln_place_of_service_cd == '31') |>
  # remove lines with adult day care service code
  filter(!(cpt_cd       %in% day_proc),
         !(service_cd   %in% day_proc),
         !(procedure_cd %in% day_proc)) |>
  distinct(<redacted>)

## Home-Based LTC Services in 2019 ---------------------------------------------
hltc_19 <- members |>
  inner_join(claims, by = c("<redacted>" = "<redacted>")) |>
  filter(claim_service_from_dt <= '2019-12-31', 
         claim_service_from_dt >= '2019-01-01') |>
  # home health codes
  filter(cpt_cd         %in% home_proc |
           service_cd   %in% home_proc |
           procedure_cd %in% home_proc |
           revenue_cd   %in% home_rev  |
           revenue_cd2  %in% home_rev  ) |>
  distinct(<redacted>)

## Home-Based LTC Services 2017-2018 -------------------------------------------
hltc_1718 <- members |>
  inner_join(claims, by = c("<redacted>" = "<redacted>")) |>
  filter(claim_service_from_dt <= '2018-12-31', 
         claim_service_from_dt >= '2017-01-01') |>
  # home health codes
  filter(cpt_cd         %in% home_proc |
           service_cd   %in% home_proc |
           procedure_cd %in% home_proc |
           revenue_cd   %in% home_rev  |
           revenue_cd2  %in% home_rev  ) |>
  distinct(<redacted>)

## Adult Day Care 2019 ---------------------------------------------------------
cltc_19 <- members |>
  inner_join(claims, by = c("<redacted>" = "<redacted>")) |>
  filter(claim_service_from_dt <= '2019-12-31', 
         claim_service_from_dt >= '2019-01-01') |>
  # keep lines with adult day care service code, pos does not matter
  filter(cpt_cd       %in% day_proc |
         service_cd   %in% day_proc |
         procedure_cd %in% day_proc) |>
  distinct(<redacted>)

## Adult Day Care 2017-2018 ----------------------------------------------------
cltc_1718 <- members |>
  inner_join(claims, by = c("<redacted>" = "<redacted>")) |>
  filter(claim_service_from_dt <= '2018-12-31', 
         claim_service_from_dt >= '2017-01-01') |>
  # keep lines with adult day care service code, pos does not matter
  filter(cpt_cd       %in% day_proc |
         service_cd   %in% day_proc |
         procedure_cd %in% day_proc) |>
  distinct(<redacted>)


## Long ILTC (4+ months) -------------------------------------------------------
### First, make member-month dataset
memb_mos <- members |> # get customer ids to join to member numbers from claims
  distinct(<redacted>, <redacted>) |> # simple crosswalk between customer id and member id
  inner_join(claims, by = c("<redacted>" = "<redacted>")) |> # join claims on member id
  # make SNF indicator and get month and year data from dates
  mutate( # note: ifelse() not performing as expected
    snf  = case_when(claim_ln_place_of_service_cd == "31" ~ 1,
                     place_of_service_cd == "31"          ~ 1,
                     TRUE                                 ~ 0),
    # flag non-snf institutional claims
    nh = case_when(claim_ln_place_of_service_cd %in% c("32", "33", "13") ~ 1,
                       place_of_service_cd %in% c("32", "33", "13")      ~ 1,
                       TRUE                                              ~ 0),
    mo_fr = month(claim_service_from_dt),
    mo_to = month(claim_service_to_dt),
    yr_fr = year(claim_service_from_dt),
    yr_to = year(claim_service_to_dt)) |>
  # get 2017-2019 data 
  # and 2020 data because stays that started in the end of 2019 won't reach the 4-month threshold until early 2020
  filter(yr_fr %in% c(2017, 2018, 2019, 2020)) |>
  distinct(<redacted>, snf, nh, yr_fr, yr_to, mo_fr, mo_to) |>
  # make each month a row for each member--creating "member-months"
  pivot_longer(cols = 4:7,
               names_to = c(".value", "dt_type"),
               names_pattern = c("(..)_(..)")) |>
  select(-dt_type) |>
  # apply flags to all rows for that month
  group_by(<redacted>, yr, mo) |>
  summarize(snf = max(snf, na.rm = TRUE),
            nh  = max(nh,  na.rm = TRUE)) |>
  ungroup() |>
  # remove the latter 3 quarters of 2020
  filter(yr %in% c(2017, 2018, 2019) |
           (yr == 2020 & mo %in% c(1,2,3)) )|>
  # remove records that don't have snf or nh indicator
  filter(snf == 1 | nh == 1) |>
  collect() 


# Find consecutive months to identify long NH stays, 
## excluding snf months when at the beginning of stays, 
## but including snf months in the middle or end of stays
iltc_4months <- memb_mos |>
  # keep only those with stays starting in 2017-2019 (can start in Dec. 2019 and extend 3 months into 2020)
  filter(yr %in% c(2017, 2018, 2019) | (yr == 2020 & mo %in% 1:3)) |>
  # mark stays with contiguous months, create visit index by customer
  mutate(mo2 = case_when(yr == 2017 ~ mo+0, # there seems to be a bug that requires adding 0 here
                         yr == 2018 ~ mo+12,
                         yr == 2019 ~ mo+24,
                         yr == 2020 ~ mo+36)
         ) |> 
  # mark stays with contiguous months
  arrange(<redacted>, mo2) |>
  group_by(<redacted>) |>
  # find the difference between months, going down the rows by customer
  mutate(diff_mo   = c(1,diff(mo2)), 
         # make visit id, it is a new visit if the month diff > 1
         visit_id = cumsum(diff_mo > 1)) |>
  ungroup() |>
  # remove snf months when they begin the stay, but not elsewhere in the stay
  group_by(<redacted>, visit_id) |>
  mutate(rn         = row_number(),
         snf_cumsum = cumsum(snf)) |>
  ungroup() |>
  filter(snf_cumsum != rn) |>
  # all remaining rows are institutional stays we want to count
  # so count the number of months for each customer and visit_id combination
  # and find the month and year that stay started
  group_by(<redacted>, visit_id) |>
  mutate(inst_months = n()) |>
  filter(inst_months >= 4) |>
  ungroup() |>
  mutate(full_date = as.Date(paste0(yr,"-",mo,"-01"))) |>
  group_by(<redacted>) |>
  summarize(yr_start = min(yr),
            min_iltc4_month = min(full_date))


# table(iltc_4months$yr_start)

## ILTC Date -------------------------------------------------------------------
# ILTC stay of any length
min_iltc_month <- memb_mos |>
  filter(nh == 1) |>
  mutate(full_date = as.Date(paste0(yr,"-",mo,"-01"))) |>
  group_by(<redacted>) |>
  summarize(min_iltc_month = min(full_date)) |>
  left_join(iltc_4months, by = "<redacted>") |>
  select(-yr_start)

## HLTC Date -------------------------------------------------------------------
min_hltc_month <- members |> # get customer ids to join to member numbers from claims
  distinct(<redacted>, <redacted>) |> # simple crosswalk between customer id and member id
  inner_join(claims, by = c("<redacted>" = "<redacted>")) |> # join claims on member id
  # make SNF indicator and get month and year data from dates
  mutate(
    home = case_when(cpt_cd       %in% home_proc ~ 1,
                     service_cd   %in% home_proc ~ 1,
                     procedure_cd %in% home_proc ~ 1,
                     revenue_cd   %in% home_rev  ~ 1,
                     revenue_cd2  %in% home_rev  ~ 1,
                     TRUE                        ~ 0)) |>
  filter(claim_service_from_dt >= "2017-01-01",
         claim_service_from_dt <= "2019-12-31",
         home == 1) |>
  group_by(<redacted>) |>
  summarize(min_hltc_month = min(claim_service_from_dt, na.rm = TRUE)) |>
  # make the first of the month to conform with the other start month-dates
  mutate(min_hltc_month = paste0(substr(as.character(min_hltc_month), 1,8),"01")) |>
  collect() 

## Death in 2019 ---------------------------------------------------------------
death_19 <- tbl(con_access,
                sql("SELECT DISTINCT <redacted>, <redacted> AS death_dt
                     FROM <redacted>.<redacted>
                     WHERE (   LOWER(<redacted>) LIKE '%death%'
                            OR LOWER(<redacted>) LIKE '%dece%')
                       AND DATE_PART(YEAR, <redacted date>) = 2019")) |>
  collect() |>
  # get rid of the one dupe who has two death dates
  group_by(<redacted>) |>
  summarize(death_dt = min(as.Date(death_dt)))
  
# Final DF =====================================================================
set.seed(789)

t1 <- Sys.time()
final_df <- members_19 |>
  distinct(<redacted>) |>
  collect() |>
  mutate(
    utiliz_19      = ifelse(<redacted> %in% pull(utiliz_19),   1, 0),
    utiliz_1718    = ifelse(<redacted> %in% pull(utiliz_1718), 1, 0),
    iltc_19        = ifelse(<redacted> %in% pull(iltc_19),     1, 0),
    iltc_1718      = ifelse(<redacted> %in% pull(iltc_1718),   1, 0),
    iltc_4m_19     = ifelse(<redacted> %in% filter(iltc_4months, yr_start ==        2019)$<redacted>, 1, 0),
    iltc_4m_1718   = ifelse(<redacted> %in% filter(iltc_4months, yr_start %in% 2017:2018)$<redacted>, 1, 0),
    snf_19         = ifelse(<redacted> %in% pull(snf_19),      1, 0),
    snf_1718       = ifelse(<redacted> %in% pull(snf_1718),    1, 0),
    hltc_19        = ifelse(<redacted> %in% pull(hltc_19),     1, 0),
    hltc_1718      = ifelse(<redacted> %in% pull(hltc_1718),   1, 0),
    cltc_19        = ifelse(<redacted> %in% pull(cltc_19),     1, 0),
    cltc_1718      = ifelse(<redacted> %in% pull(cltc_1718),   1, 0)
  ) |>
  left_join(enrollment_months, by = "<redacted>") |>
  mutate(enroll_24mo_1718 = ifelse(months_enrolled == 24, 1, 0),
         enroll_12mo_1718 = ifelse(months_enrolled >= 12, 1, 0),
         enroll_6mo_1718  = ifelse(months_enrolled >=  6, 1, 0)
  ) |>
  # join the index months for iltc, hltc, and death
  left_join(min_iltc_month, by = "<redacted>") |>
  left_join(min_hltc_month, by = "<redacted>") |>
  left_join(death_19,       by = "<redacted>")
t2 <- Sys.time()
t2-t1 # 1 to 9 mins

# check dupes
length(unique(final_df$<redacted>))

final_df2 <- final_df |>
  rename(cust_id = <redacted>) |>
  # make index dates
  mutate(across(c(min_iltc_month, 
                  min_iltc4_month, 
                  min_hltc_month,
                  death_dt), as.Date),
         random_month = as.Date(paste0("2019-",sample(x = 1:12, size = nrow(final_df), replace = TRUE ),"-01")),
         index_dt_01 = case_when(
           # those who started a long iltc stay in 2019, use that start as their index date
           iltc_4m_19 == 1 & min_iltc4_month >= as.Date("2019-01-01") ~ min_iltc4_month,
           # if not above, but they had a short iltc stay in 2019, use that start as their index date
           iltc_19    == 1 & min_iltc_month  >= as.Date("2019-01-01") ~ min_iltc_month,
           # for those who had hltc in 2019, use that start as their index date
           hltc_19    == 1 & min_hltc_month  >= as.Date("2019-01-01") ~ min_hltc_month,
           # for those who died in 2019, use that as their index date
           death_19   == 1 & death_dt        >= as.Date("2019-01-01") ~ death_dt,
           # for everyone else, randomly assign a month in 2019 as their index date
           TRUE ~ random_month),
         # now get the last day in the month as the index date (instead of the first of that month)
         index_dt = lubridate::ceiling_date(index_dt_01, "month") - lubridate::days(1)
  ) |>
  # remove 01 index date column
  select(-index_dt_01) |>
  # make final category
  mutate(ltc_cat = case_when(months_enrolled < 20 | utiliz_19 == 0 ~ "Not in study",
                             iltc_4m_19 == 1 & iltc_4m_1718 == 0   ~ "ILTC long 2019, start 2019",
                             iltc_4m_19 == 1 & iltc_4m_1718 == 1   ~ "ILTC long 2019, start 17-18",
                             iltc_19    == 1 & iltc_1718    == 0   ~ "ILTC short 2019, start 2019",
                             iltc_19    == 1 & iltc_1718    == 1   ~ "ILTC short 2019, start 17-18",
                             hltc_19    == 1 & hltc_1718    == 0   ~ "HLTC 2019, start 2019",
                             hltc_19    == 1 & hltc_1718    == 1   ~ "HLTC 2019, start 17-18",
                             iltc_19 == 0 & hltc_19 == 0 &  iltc_1718 == 0 & hltc_1718 == 0  ~ "No LTC 2019, nor 17-18",
                             iltc_19 == 0 & hltc_19 == 0 & (iltc_1718 == 1 | hltc_1718 == 1) ~ "No LTC 2019, but LTC in 17-18")) 


## Save data -------------------------------------------------------------------
aws.s3::s3write_using(final_df2,
                      write.csv,
                      row.names = FALSE,
                      bucket = "<redacted>",
                      object = "<redacted>/df_for_popn_counts.csv")



## Done ----
