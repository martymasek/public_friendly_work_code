# Mother-Baby Linking Logic ----

# Setup ----
source(here::here("code/common/setup.R"))
source(here::here("code/common/connect.R"))

# Load Cohort ----
cohort <- s3read_using(read.csv,
                       bucket = "<redacted>",
                       object = "<redacted>/data/cohort/cohort.csv")

# Get Head of Household ID during Delivery ----
hh_ids <- tbl(con_access,
              sql("SELECT * FROM <redacted>.<redacted>")) |>
  # get all membership data for moms in the cohort
  filter(<redacted> %in% local(cohort$person_id)) |>
  # keep only fields I'll need for this chunk
  select(<redacted>, 
         <redacted>,
         <redacted date field>,
         <redacted date field>,
         <redacted date field>,
         <redacted date field>
         ) |>
  # bring into memory, so I can filter on dates with the cohort data
  collect() |>
  # join the cohort data
  right_join(cohort,
             by = c("<redacted>" = "person_id")) |>
  # keep the membership rows that are active during the delivery
  filter(between(deliv_start_dt,
                 <redacted date field>,
                 <redacted date field>),
         between(deliv_start_dt,
                 <redacted date field>,
                 <redacted date field>)) |>
  # for the few with two rows active simultaneously, keep the more recent row
  group_by(<redacted>) |>
  mutate(max_eff_dt = max(<redacted date field>)) |>
  ungroup() |>
  filter(<redacted date field> == max_eff_dt) |>
  # keep the household ID and customer number only
  distinct(household_id, person_id)

# check dupes 
hh_ids |> count() - hh_ids |> distinct(person_id) |> count()

# # find those without hh_id data
# setdiff(cohort$person_id, hh_ids$person_id)

# Get all membership rows associated with the household ID, including the infants'.
## Add birth date to find infants.
membership <- tbl(con_access,
                  sql("SELECT * FROM <redacted>.<redacted>")) |>
  select(household_id, 
         person_id, 
         <redacted date field>,
         <redacted date field>,
         <redacted date field>,
         <redacted date field>) |>
  filter(household_id %in% local(hh_ids$household_id)) |>
  left_join(tbl(con_access, sql("SELECT <redacted>, birth_dt FROM <redacted>.<redacted>")),
            by = c("<redacted>" = "person_pk")) |>
  collect()

# make the linked mother-infant customer IDs
linked_cust_ids <- cohort |>
  rename(mother_id = person_id) |>
  select(mother_id, deliv_start_dt, deliv_end_dt, deliv_claims_days) |>
  mutate(across(c(deliv_start_dt, deliv_end_dt), as.Date)) |>
  left_join(hh_ids, 
            by = c("mother_id" = "person_id")) |>
  left_join(membership,
            by = "household_id",
            multiple = "all") |>
  filter(between(birth_dt,
                 deliv_start_dt - days(7),
                 deliv_end_dt   + days(7))) |>
  # rename the infant's id
  # use the infant's birth date to mark the delivery date more accurately
  rename(infant_id = person_id,
         infant_birth_dt = birth_dt) |>
  select(household_id,
         mother_id,
         infant_id,
         infant_birth_dt
         ) |>
  distinct()

cohort |>
  mutate(linked_infant = ifelse(person_id %in% linked_cust_ids$mother_id, 1, 0)) |>
  group_by(groups, linked_infant) |>
  summarize(n = n()) |>
  ungroup() |>
  group_by(groups) |>
  mutate(prop = n/sum(n))


# write linked mother-infant ids to s3
s3write_using(linked_cust_ids,
              write.csv,
              row.names = FALSE,
              bucket    = "<redacted>",
              object    = "<redacted>/data/cohort/linked_mom_baby_ids.csv")
