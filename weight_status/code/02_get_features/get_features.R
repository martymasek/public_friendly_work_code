# Gather Features

# 0. Setup ---------------------------------------------------------------------
options(scipen = 999)

# libraries
library(plyr) # yes, load this and make sure it's before dbplyr
source(here::here("code/common/setup.R"))

# DB connections
source(here::here("code/common/connect.R"))

# Load cohort
full_cohort <- s3read_using(read.csv,
                       bucket = "<redacted>",
                       object = "<redacted>/data/dev/target/full_cohort_and_labels.csv")

# 1. Get Features --------------------------------------------------------
## Get a single row for each current member
## Chunked Features on Server ----
## For features created and joined on the server: do in chunks because 
### writing a temp table of all 500K takes too long
## Many of these SQL queries are from the Feature Generator, others are new

# Split cohort into chunks since cannot write large temp table to server
chunks <- split(full_cohort, (seq(nrow(full_cohort))-1) %/% 20000) 

## Make list to store results
chunk_feats <- list()

### Make list with sql query paths composing each element
queries <- paste0("<redacted>/sql/queries-for-feat-func/",
                  list.files("<redacted>/sql/queries-for-feat-func")) |>
  as.list()
  
# Make function to run queries
runQuery <- function(get_query = q, lkbk_months = 18) {
  query <- 
    read_file(here(queries[q])) %>% # query to get features, must use older pipe for . substitution below
    gsub("lkbk_months_features",    # replace text in sql query
         lkbk_months,               # number of months prior to index date to search for claims
         .)
  
  # get results
  tbl(con_access,
      sql(query)) |>
    collect()
}

## Get features for people in each chunk
t1 <- Sys.time()
for (i in 1:length(chunks)) {
  
  # get chunk and make it a dataframe
  cohort_chunk <- chunks[i] |> plyr::ldply(.fun = data.frame, .id = NULL)
  
  # write to temp table on server
  ## remove the temp table if it exists
  if (DBI::dbExistsTable(con_access, "#cohort_chunk")) {
    DBI::dbRemoveTable(con_access, "#cohort_chunk")
  } 
  
  ## write the temp table
  dbWriteTable(con_access,
               "#cohort_chunk",
               cohort_chunk,
               temporary = TRUE)
  
  # make a list to store query results
  results <- list()
  
  for (q in 1:length(queries)) {
    results[q] <- runQuery() |> list()
  }
  
  chunk_feats[i] <- results |> purrr::reduce(full_join, by = "cust_id") |> list()
  

  # print progress
  print(paste0("Finished chunk ", i,"."))
}

t2 <- Sys.time()
t2-t1


## HH weight features ----
# get most recent hh id, since they change over time
hh_ids <- tbl(con_access,
              sql(read_file("code/get-features/sql/hh_ids.sql"))) |>
  collect()

# get the number of hh members with each weight status by hh_id
hh_weight_status <- full_cohort |>
  left_join(hh_ids, by = "cust_id") |>
  filter(!(is.na(hh_id))) |>
  select(hh_id,
         obese) |>
  mutate(hh_nonobese = ifelse(obese == 0, 1, 0)) |> 
  rename(hh_obese    = obese) |>
  group_by(hh_id) |>
  summarise(across(c(hh_obese, hh_nonobese), sum))

# join the hh numbers to individuals in scoring group and 
# remove oneself from the hh count for their own weight status, where applicable
feat_hh_weight <- full_cohort |>
  left_join(hh_ids, by = "cust_id") |>
  select(cust_id, hh_id, obese) |>
  # join household data
  left_join(hh_weight_status, by = "hh_id") |>
  # remove self from hh weight status columns
  mutate(hh_obese    = case_when(obese == 1 ~ hh_obese-1,    
                                 TRUE       ~ hh_obese),
         hh_nonobese = case_when(obese == 0 ~ hh_nonobese-1, 
                                 TRUE       ~ hh_nonobese)) |>
  # now make these into binary instead of count measures and fill NA with 0
  mutate(hh_obese    = case_when(hh_obese    >= 1 ~ 1, TRUE ~ 0),
         hh_nonobese = case_when(hh_nonobese >= 1 ~ 1, TRUE ~ 0)) |>
  select(cust_id, hh_id, hh_obese, hh_nonobese)

rm(hh_weight_status)

## Load CDC Obesity Data ----
# load in the cdc data
cdc_zip <- s3read_using(read.csv,
                        colClasses = c("character", "numeric"),
                        bucket = "<redacted>",
                        object = "<redacted>/data/dev/incoming/cdc_obesity_zip.csv"
)
cdc_tract <- s3read_using(read.csv,
                          colClasses = c("character", "numeric"),
                          bucket = "<redacted>",
                          object = "<redacted>/data/dev/incoming/cdc_obesity_tract.csv"
)

# 2. Combine data ----
final <- chunk_feats |> 
  # collapse the list into a df
  bind_rows() |>
  # join hh weight data
  left_join(feat_hh_weight, by = "cust_id") |>
  # join on tract if not missing, otherwise join on zip code tabulation area
  left_join(cdc_tract, by = c("fips_census_tract_cd" = "tract")) |>
  left_join(cdc_zip,   by = c("zip_cd" = "zip")) |>
  mutate(cdc_obese_pct = ifelse(is.na(obese_pct_tract), obese_pct_zip, obese_pct_tract)) |>
  select(-c(zip_cd,
            fips_census_tract_cd))




# 3. Write table to S3 ------------------------------------------------------------
s3write_using(x = final,
              FUN = arrow::write_parquet,
              bucket = "<redacted>",
              object = "<redacted>/data/dev/features/full_unrefined_featureset.parquet"
)

# Finishing Steps --------------------------------------------------------------
DBI::dbDisconnect(con_access)
renv::snapshot()
