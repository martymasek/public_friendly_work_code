# Identify Cohort for the <redacted> Maternity Evaluation

# Setup ------------------------------------------------------------------------

source(here::here("code/common/setup.R"))
source(here::here("code/common/connect.R"))

# Get Risk Score Data ----------------------------------------------------------
## Everyone in the evaluation pool must have a risk score because members are
## referred to the program based on their risk score.
scores <- tbl(con_ds,
              sql("SELECT * FROM <redacted>.<redacted>_risk_scores")) |>
  filter(!is.na(insight_score_result_cd),
         between(insight_score_run_dt, as.Date("2021-10-01"), as.Date("2022-08-31"))
  )

# Keep the rows with "High Risk" scores ----
high_risk <- scores |>
  # high risk can be as low as >.10 even though business rule was supposed to be > .15
  filter(insight_score_result_cd > .10) |> 
  collect()


# Get Program Tracking Data (and join) -----------------------------------------

## get the care management program tracking data
### don't restrict file date yet (to make sure not to miss anyone who was treated)
### don't restrict the program name yet (to know who went to different programs)
cm <- tbl(con_access,
          sql("SELECT * FROM <redacted>.<redacted>_program_tracking")) |>
  filter(hfcares_population == "maternity"
         # current_risk_group == "HIGH"
         # program_name == "Maternity High Risk"
  ) |>
  collect()


# join score and CM data
scores_cm <- high_risk |>
  full_join(cm, 
            by = c("<redacted>" = "<redacted>"),
            multiple = "all") |>
  # keep those with either no CM data or the CM file date >= score date
  filter(is.na(file_date) | is.na(insight_score_run_dt) | file_date >= insight_score_run_dt)

## Some filtering ----
## Remove those without score data
## and remove those whose file date is > 60 days past their score date
filled_scores_cm <- scores_cm |>
  filter(!is.na(insight_score_result_cd)) |>
  # either they were not sent to CM 
  # or if they were sent, the file date is within 60 days of the risk score date
  filter(is.na(file_date) | between(file_date,
                                    insight_score_run_dt,
                                    insight_score_run_dt + days(60))) |>
  # keep only important fields and get distinct rows
  select(<redacted>,
         starts_with("insight"),
         trimester,
         file_date,
         starts_with("program"),
         starts_with("assessment"),
         targeted,
         outreached,
         engaged
  ) |>
  distinct()


# Get delivery data ------------------------------------------------------------
## Find deliveries in steps, using best codes first, 
## then using other codes to search for those not found first

## Find with standard ICD-10 CM codes ----

# list codes that indicate delivery
deliv_icd_z <- paste0("Z", 37:38)
## Encounter for delivery (O80-O84)
deliv_icd_o8 <- paste0("O", 80:84)

delivs_i1 <- tbl(con_access,
                in_schema("<redacted>", "<redacted>")) |>
  filter(person_id %in% local(unique(filled_scores_cm$<redacted>))) |>  # reduce size
  filter(between(claim_service_from_dt, 
                 as.Date("2021-11-01"), # from month of program start
                 local(Sys.Date()))) |> # to today to capture as many deliveries as possible
  select(person_id, 
         claim_service_from_dt, 
         claim_service_to_dt, 
         matches("^diagnosis_\\d+_cd")) |>
  pivot_longer(cols = starts_with("diagnosis"),
               names_to = "diag_num",
               values_to = "diag_cd") |>
  filter(substr(diag_cd, 1, 3) %in% local(c(deliv_icd_z, deliv_icd_o8))) |>
  mutate(id_method = "icd10_1st") |>
  rename(id_code = diag_cd) |>
  select(-diag_num) |>
  distinct() |>
  compute()

## Find with DRGS ----
## DRGs from SMEs
deliv_drg <- c("540","541","542",
               "560",
               "768",
               "783","784","785","786","787","788",
               "796","797","798",
               "805","806","807")

delivs_d <- tbl(con_access,
                in_schema("<redacted>", "claim_header")) |>
  filter(person_id %in% local(filled_scores_cm$<redacted>)) |>  # reduce size
  filter(between(claim_service_from_dt, 
                 as.Date("2021-11-01"), # from month of program start
                 local(Sys.Date()))) |>
  anti_join(delivs_i1, by = "<redacted>") |> # remove those already identified
  select(<redacted>, 
         claim_service_from_dt, 
         claim_service_to_dt, 
         diagnosis_related_group_cd,
         claim_header_sub_drg_cd) |>
  filter(substr(diagnosis_related_group_cd, 1, 3) %in% local(deliv_drg) |
           substr(claim_header_sub_drg_cd, 1, 3) %in% local(deliv_drg)) |>
  mutate(id_method = "drg",
         id_code = case_when(!is.na(diagnosis_related_group_cd) ~ diagnosis_related_group_cd,
                             TRUE                               ~ claim_header_sub_drg_cd)) |>
  select(<redacted>,
         claim_service_from_dt,
         claim_service_to_dt,
         id_method,
         id_code) |>
  distinct() |>
  compute()

## Find with secondary ICD-10 codes ----
## Complications of labor and delivery (O60-O77) 
deliv_icd_o67 <- paste0("O", 60:77)
## complications during "childbirth"
deliv_cbirth <- readxl::read_excel(here("docs/ref-code-lists/mcd_gov_mat_inf_hlth_ref_codes_2022.xlsx"),
                           sheet = "1 - Ever pregnant",
                           skip = 1) |>
  filter(Type == "ICD-10-CM",
         str_detect(Definition, "in childbirth") | 
           str_detect(Definition, "complicating childbirth"))  |>
  mutate(Code2 = paste0(substr(Code, 1, 3), ".", substr(Code, 4, 10))) |> 
  pull(Code2)

delivs_i2 <- tbl(con_access,
              in_schema("<redacted>", "claim_header")) |>
  filter(person_id %in% local(unique(filled_scores_cm$<redacted>))) |>  # reduce size
  filter(between(claim_service_from_dt, 
                 as.Date("2021-11-01"), # from month of program start
                 local(Sys.Date()))) |> # to today to capture as many deliveries as possible
  anti_join(delivs_i1, by = "<redacted>") |> # remove those already identified
  anti_join(delivs_d,  by = "<redacted>") |>
  select(<redacted>, 
         claim_service_from_dt, 
         claim_service_to_dt, 
         matches("^diagnosis_\\d+_cd")) |>
  pivot_longer(cols = starts_with("diagnosis"),
               names_to = "diag_num",
               values_to = "diag_cd") |>
  filter(substr(diag_cd, 1, 3) %in% local(c(deliv_icd_o67, deliv_cbirth))) |>
  mutate(id_method = "icd10_2nd") |>
  rename(id_code = diag_cd) |>
  select(-diag_num) |>
  distinct() |>
  compute()

## Find with revenue codes ----
## Labor and delivery room
deliv_rev <- readxl::read_excel(here("docs/ref-code-lists/mcd_gov_mat_inf_hlth_ref_codes_2022.xlsx"),
                           sheet = "4 - L&D, outcome unknown",
                           skip = 1) |>
  filter(Type == "UBREV") |> 
  pull(Code)

delivs_r <- tbl(con_access,
                in_schema("<redacted>", "claim_lines")) |>
  filter(person_id %in% local(filled_scores_cm$<redacted>)) |>  # reduce size
  filter(between(claim_line_service_start_dt, 
                 as.Date("2021-11-01"), # from month of program start
                 local(Sys.Date()))) |> # to today to capture as many deliveries as possible
  anti_join(delivs_i1, by = "<redacted>") |> # remove those already identified
  anti_join(delivs_d,  by = "<redacted>") |>
  anti_join(delivs_i2, by = "<redacted>") |>
  select(<redacted>, 
         claim_line_service_start_dt, 
         claim_line_service_end_dt, 
         revenue_cd) |>
  filter(revenue_cd %in% local(deliv_rev)) |>
  mutate(id_method = "revenue") |>
  rename(id_code = revenue_cd) |>
  distinct() |>
  compute()

## Find with CPT codes ----
## Definition contains "delivery"
deliv_cpt <- readxl::read_excel(here("docs/ref-code-lists/mcd_gov_mat_inf_hlth_ref_codes_2022.xlsx"),
                                sheet = "1 - Ever pregnant",
                                skip = 1) |>
  filter(Type == "CPT",
         str_detect(tolower(Definition), "delivery")) |> 
  pull(Code)

delivs_c <- tbl(con_access,
                in_schema("<redacted>", "claim_ln_detail")) |>
  left_join(tbl(con_access, sql("SELECT <redacted> AS person_id, <redacted> FROM <redacted>.<redacted>")),
            by = c("<redacted>" = "<redacted>")) |>
  filter(person_id %in% local(filled_scores_cm$<redacted>)) |>  # reduce size
  filter(between(claim_ln_service_from_dt, 
                 as.Date("2021-11-01"), # from month of program start
                 local(Sys.Date()))) |> # to today to capture as many deliveries as possible
  anti_join(delivs_i1, by = "<redacted>") |> # remove those already identified
  anti_join(delivs_d,  by = "<redacted>") |>
  anti_join(delivs_i2, by = "<redacted>") |>
  anti_join(delivs_r,  by = "<redacted>") |>
  select(<redacted>, 
         claim_ln_service_from_dt, 
         claim_ln_service_to_dt, 
         service_cd) |>
  filter(service_cd %in% local(deliv_cpt)) |>
  mutate(id_method = "cpt") |>
  rename(id_code = service_cd) |>
  distinct() |>
  compute()

## Combine Delivery Data ----

delivs <- bind_rows(collect(delivs_i1),
                    collect(delivs_d),
                    collect(delivs_i2),
                    collect(delivs_r),
                    collect(delivs_c)) 

table(delivs$id_method)

## Delivery Date Data ----
# calculate the claim date span for each delivery
deliv_span <- delivs |>
  group_by(<redacted>) |>
  arrange(claim_service_from_dt, 
          claim_service_to_dt) |>
  mutate(
    # get number of days between claims
    time_between = as.integer(claim_service_from_dt - lag(claim_service_from_dt)), 
    # if more than 20 weeks (min. gest. age), assume new delivery
    new_deliv_ind = case_when(time_between > 140 ~ 1, TRUE ~ 0),  
    # make index for delivery number/order, starts with 0
    deliv_id = cumsum(new_deliv_ind)) |> 
  ungroup() 

## Summarize Deliveries to Single Rows ----
# for each person-delivery, get one row (summarize dates and singleton/multiple data)
deliv_span_distinct <- deliv_span |>
  group_by(person_id, deliv_id) |>
  mutate(singleton_o = ifelse(substr(id_code, 1, 3) %in% paste0("O",80:83), 1, 0),
         multiple_o  = ifelse(substr(id_code, 1, 3) %in% c("O84"), 1, 0),
         singleton_z = ifelse(substr(id_code, 1, 5) %in% c(paste0("Z37.", 0:1),
                                                           paste0("Z38.", 0:2)), 1, 0),
         multiple_z  = ifelse(substr(id_code, 1, 5) %in% c(paste0("Z37.", 2:8),
                                                           paste0("Z38.", 3:8)), 1, 0)
  )|>
  summarize(deliv_start_dt = min(claim_service_from_dt),
            deliv_end_dt   = max(claim_service_to_dt),
            singleton_o    = max(singleton_o, na.rm = TRUE),
            multiple_o     = max(multiple_o,  na.rm = TRUE),
            singleton_z    = max(singleton_z, na.rm = TRUE),
            multiple_z     = max(multiple_z,  na.rm = TRUE),
            .groups = "keep") |>
  ungroup() |>
  mutate(deliv_claims_days = deliv_end_dt - deliv_start_dt,
         singleton = case_when(singleton_z == 1 ~ 1,
                               singleton_o == 1 ~ 1,
                               multiple_z  == 1 ~ 0,
                               multiple_o  == 1 ~ 0,
                               TRUE ~ NA))

# Deduplicate Score Data ----
## deduplicate members based on different criteria (depending on their data)
# verified there are no duplicates introduced because of dupes in the cm data
# dupes are introduced because of multiple scoring

## first get cust ids who have a single record
single_record_mems_df <- filled_scores_cm |>
  group_by(<redacted>) |>
  mutate(n = n()) |>
  filter(n == 1) |>
  select(-n)

## for those who have multiple records and were sent to cm, (dupe is from insight df),
## get the record for the insight date closest to--but not past--the cm file date
multi_rec_cm_df <- filled_scores_cm |>
  filter(!<redacted> %in% single_record_mems_df$<redacted>,
         !is.na(file_date),
         insight_score_run_dt <= file_date) |>
  group_by(<redacted>) |>
  arrange(desc(insight_score_run_dt)) |>
  mutate(rn = row_number()) |>
  ungroup() |>
  filter(rn == 1) |>
  select(-rn)

# finally get the most recent record for those who were not sent to cm, 
# keeping only those who were scored in accordance with the business rule to score only twice two weeks in a row
multi_rec_no_cm_df <- filled_scores_cm |>
  filter(!<redacted> %in% single_record_mems_df$<redacted>,
         !<redacted> %in% multi_rec_cm_df$<redacted>) |>
  group_by(<redacted>) |>
  mutate(nrows = n(),
         max_score_dt = max(insight_score_run_dt),
         min_score_dt = min(insight_score_run_dt)) |>
  ungroup()|>
  # only keep those that followed business rules to score twice
  filter(nrows == 2,
         # and keep those whose two scores are not more than 30 days apart
         max_score_dt - min_score_dt <= days(30)
  ) |>
  # and keep the latest score
  filter(insight_score_run_dt == max_score_dt) |>
  select(-c(nrows,
            max_score_dt, 
            min_score_dt))

# Make Cohort Pool -------------------------------------------------------------
## Join deliveries to cohort
## Remove those without delivery
## For the CM folks, delivery must be after the score date
## Make study group categories
## Separate those with intervention within 60 days of delivery, might be too late
## Separate those who were sent but not to Maternity High Risk program
cohort <- bind_rows(
  single_record_mems_df,
  multi_rec_cm_df,
  multi_rec_no_cm_df) |>
  rename(person_id = <redacted>) |>
  left_join(deliv_span_distinct,
            by = "person_id",
            multiple = "all") |>
  filter(
    # remove those with no delivery
    # and remove those whose delivery is before the score date
    insight_score_run_dt < deliv_start_dt,
    # and remove the delivery rows that are > 40 weeks after the score date
    between(deliv_start_dt,
            insight_score_run_dt,
            insight_score_run_dt + weeks(40))) |> 
  # and finally remove those who still have more than one delivery date associated with a score date
  group_by(person_id) |>
  mutate(n = n()) |> 
  filter(n == 1) |> # removing 4 members
  mutate(
    # make some variables to use for categorization
    days_between_assess_deliv = as.integer(deliv_start_dt - as.Date(assessment_complete_date)),
    # when defining groups, use only "engaged" as indicator of treatment (engagement level, program close statuses)
    groups = case_when(
      is.na(file_date) ~ "Not Sent",
      engaged == 1 & 
        program_name == "Maternity High Risk" & 
        days_between_assess_deliv > 60 ~ "Engaged MHR",
      engaged == 1 & program_name == "Maternity High Risk" ~ "Engaged MHR, too late?",
      engaged == 1 ~ "Engaged Other Program",
      !is.na(file_date) ~ "Sent, Not Engaged"
    )) |>
  select(-c(deliv_id,
            singleton_o,
            multiple_o,
            singleton_z,
            multiple_z,
            n))


# Write to s3 ----
s3write_using(x         = cohort,
              FUN       = write.csv,
              row.names = FALSE,
              quote     = TRUE,
              bucket    = "<redacted>",
              object    = "<redacted>/data/cohort/cohort.csv")
 
# Done!
