# make the date intervals during which to measure outcomes

# Outcome interval options:
## A. mother's last menstrual to six months after delivery (mother and baby)
## B. mother's last menstrual to delivery-associated discharge date(s) (mother and baby)
## C. mother's delivery-associated admit to delivery-associated discharge date(s) (mother and baby)

# try to estimate these from the delivery date, then refine in a later step with claims if possible

# Setup ----
source(here::here("code/common/setup.R"))
source(here::here("code/common/connect.R"))

# Load Data ----
cohort <- s3read_using(
  read.csv,
  bucket = "<redacted>",
  object = "<redacted>/data/cohort/cohort.csv") 

## get mother and baby ids
linked_cust_ids <- s3read_using(
  read.csv,
  bucket    = "<redacted>",
  object    = "<redacted>/data/cohort/linked_mom_baby_ids.csv") 

# Determine delivery date and claim windows ----
claim_dates <- cohort |>
  rename(mother_id = person_id) |>
  # for those with linked infants, join the infant's birth date (need distinct() because of twins, etc.)
  left_join(linked_cust_ids |> distinct(mother_id, infant_birth_dt), 
            by = "mother_id",
            multiple = "all") |> # some have twins with birthdays one day apart
  mutate(across(c(infant_birth_dt,
                  deliv_start_dt), as.Date),
         delivery_dt = case_when(!is.na(infant_birth_dt) ~ infant_birth_dt,
                                 deliv_claims_days <= 60 ~ deliv_start_dt),
         claim_window_start_dt = delivery_dt - weeks(44),
         claim_window_end_dt   = delivery_dt + weeks(26)) |>
  select(mother_id,
         delivery_dt,
         claim_window_start_dt,
         claim_window_end_dt) |>
  # account for the twins with two delivery dates to get one row for the mother
  group_by(mother_id) |>
  summarize(delivery_dt           = min(delivery_dt),
            claim_window_start_dt = min(claim_window_start_dt),
            claim_window_end_dt   = max(claim_window_end_dt))

# Write to s3 ----
s3write_using(claim_dates,
              write.csv,
              row.names = FALSE,
              bucket = "<redacted>",
              object = "<redacted>/data/cohort/outcome_date_intervals.csv")

### Examine delivery date assignment ----
# what is the distribution of delivery dates?
hist(claim_dates$delivery_dt, breaks = "weeks")

# how many have no defined delivery date?
nrow(filter(claim_dates, is.na(delivery_dt)))
# for those with no defined delivery date, what is their group and what is their span of delivery-related claim
# claim_dates |> filter(is.na(delivery_dt)) |> select(deliv_claims_days, groups)
# that is acceptable

# how many are assigned the delivery date without an infant linked birth date?
# cohort2 |> filter(is.na(infant_birth_dt), !is.na(delivery_dt)) |> count()
# cohort2 |> filter(is.na(infant_birth_dt), !is.na(delivery_dt)) |> group_by(groups) |> tally()
