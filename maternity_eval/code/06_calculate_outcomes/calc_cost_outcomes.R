# Calculate Cost (Outcome) for <redacted> Eval -----

# Setup ----
source(here::here("code/common/setup.R"))
source(here::here("code/common/connect.R"))

# get claim date intervals
claim_dates <- s3read_using(
  read.csv,
  colClasses = c("character", rep("Date", 3)),
  bucket = "<redacted>",
  object = "<redacted>/data/cohort/outcome_date_intervals.csv")

# load linked mother and baby ids
linked_cust_ids <- s3read_using(
  read.csv,
  colClasses = c(rep("character", 3), "Date"),
  bucket    = "<redacted>",
  object    = "<redacted>/data/cohort/linked_mom_baby_ids.csv")

# load claim headers
claim_headers <- s3read_using(
  arrow::read_parquet,
  bucket    = "<redacted>",
  object    = "<redacted>/data/deduped-claims/claim_headers.parquet") |>
  mutate(across(ends_with("_dt"), as.Date))


# Cost of claims on or spanning delivery date ----
## mother admit discharge spanning delivery date
mother_admit_disch_cost <- claim_dates |>
  left_join(claim_headers,
            by = c("mother_id" = "<redacted>"),
            multiple = "all") |>
  # filter(claim_tp == "U",
  #        place_of_service %in% c()) |>
  filter(between(delivery_dt,
                 admission_dt,
                 discharge_dt)) |>
  group_by(mother_id) |>
  summarize(m_admit_disch_cost = sum(<redacted>),
            m_admit_disch_patient_cost = sum(<redacted>))

mother_admit_disch_minus1_cost <- claim_dates |>
  left_join(claim_headers,
            by = c("mother_id" = "<redacted>"),
            multiple = "all") |>
  filter(between(delivery_dt,
                 admission_dt - days(1),
                 discharge_dt)) |>
  group_by(mother_id) |>
  summarize(m_admit_minus1_cost = sum(<redacted>),
            m_admit_minus1_patient_cost = sum(<redacted>))

mother_span_deliv_cost <- claim_dates |>
  left_join(claim_headers,
            by = c("mother_id" = "<redacted>"),
            multiple = "all") |>
  filter(between(delivery_dt,
                 claim_service_from_dt,
                 claim_service_to_dt)) |>
  group_by(mother_id) |>
  summarize(m_span_deliv_cost = sum(<redacted>),
            m_span_deliv_patient_cost = sum(<redacted>))

mother_span_deliv_minus1_cost <- claim_dates |>
  left_join(claim_headers,
            by = c("mother_id" = "<redacted>"),
            multiple = "all") |>
  filter(between(delivery_dt,
                 claim_service_from_dt - days(1),
                 claim_service_to_dt)) |>
  group_by(mother_id) |>
  summarize(m_deliv_minus1_cost = sum(<redacted>),
            m_deliv_minus1_patient_cost = sum(<redacted>))

## baby admit and dischage spanning birth date
baby_admit_disch_cost <- claim_dates |>
  left_join(linked_cust_ids, 
            by = "mother_id",
            multiple = "all") |>
  filter(!is.na(infant_id)) |>
  left_join(claim_headers,
            by = c("infant_id" = "<redacted>"),
            multiple = "all") |>
  filter(between(delivery_dt,
                 admission_dt,
                 discharge_dt)) |>
  group_by(mother_id) |>
  summarize(b_admit_disch_cost = sum(<redacted>),
            b_admit_disch_patient_cost = sum(<redacted>))

baby_admit_plus7_cost <- claim_dates |>
  left_join(linked_cust_ids, 
            by = "mother_id",
            multiple = "all") |>
  filter(!is.na(infant_id)) |>
  left_join(claim_headers,
            by = c("infant_id" = "<redacted>"),
            multiple = "all") |>
  filter(admission_dt <= delivery_dt + days(7)) |> # admit is on birth date or up to 7 days after birth
  group_by(mother_id) |>
  summarize(b_admit_plus7_cost = sum(<redacted>),
            b_admit_plus7_patient_cost = sum(<redacted>))


## baby (service on or spanning birth date)
baby_span_birth_cost <- claim_dates |>
  left_join(linked_cust_ids, 
            by = "mother_id",
            multiple = "all") |>
  filter(!is.na(infant_id)) |>
  left_join(claim_headers,
            by = c("infant_id" = "<redacted>"),
            multiple = "all") |>
  filter(between(delivery_dt,
                 claim_service_from_dt,
                 claim_service_to_dt)) |>
  group_by(mother_id) |>
  summarize(b_span_birth_cost = sum(<redacted>),
            b_span_birth_patient_cost = sum(<redacted>))

## baby (service on or spanning birth date, plus 7 days)
baby_span_birth_7_cost <- claim_dates |>
  left_join(linked_cust_ids, 
            by = "mother_id",
            multiple = "all") |>
  filter(!is.na(infant_id)) |>
  left_join(claim_headers,
            by = c("infant_id" = "<redacted>"),
            multiple = "all") |>
  filter(claim_service_from_dt <= delivery_dt + days(7) # claim service date starts up to 7 days after birth
                 ) |>
  group_by(mother_id) |>
  summarize(b_birth_plus7_cost = sum(<redacted>),
            b_birth_plus7_patient_cost = sum(<redacted>))

# stop here for now, but come back to make more outcomes and refine these later
## especially add the "episode"-based delivery date window once logic is approved

# join outcomes so far

joined_outcomes <- claim_dates |>
  select(mother_id) |>
  left_join(mother_admit_disch_cost, by = "mother_id") |>
  left_join(mother_admit_disch_minus1_cost, by = "mother_id") |>
  left_join(mother_span_deliv_cost,  by = "mother_id") |>
  left_join(mother_span_deliv_minus1_cost, by = "mother_id") |>
  left_join(baby_admit_disch_cost,   by = "mother_id") |>
  left_join(baby_admit_plus7_cost,   by = "mother_id") |>
  left_join(baby_span_birth_cost,    by = "mother_id") |>
  left_join(baby_span_birth_7_cost,  by = "mother_id")

# write to s3
s3write_using(joined_outcomes,
              write.csv,
              row.names = FALSE,
              bucket    = "<redacted>",
              object    = "<redacted>/data/outcomes/outcomes_cost.csv"
              )

# Done
