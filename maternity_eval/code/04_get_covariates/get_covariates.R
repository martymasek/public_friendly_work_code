# Get Features from Risk Model from S3 ----

# Setup ----
source(here::here("code/common/setup.R"))
source(here::here("code/common/connect.R"))

# Load cohort ----

## load cohort data
cohort <- s3read_using(
  read.csv,
  bucket = "<redacted>",
  object = "<redacted>/data/cohort/cohort.csv")

### List of essential features at time of scoring
## 1. risk score 
## 2. gestational age or trimester at scoring
## 3. singleton pregnancy
## 4. age
## 5. company code (not necessary, since all but 4 have medicaid)
## 6. county

### List of important but non-essantial features at time of scoring
# a. race/ethnicity
# b. language
# c. <redacted>
# d. <redacted>
# e. <redacted>
# f. <redacted>
# g. coc
# h. <redacted>
# i. <redacted>
# j. conditions: mental health, substance use, blood pressure, t2 diabetes, prediabetes, weight status
# k. sex of fetus


# Get features used for scoring ----

## set up for the loop
feat_dir <- "s3:<redacted>/"
features <- list()
time_log <- list()

# run time 1.5 hours for 2-3k records, ok for a single run
for (cust_idx in 1:nrow(cohort)) {
  
  # format score dates to match the file names
  feat_dt  <- paste0(substr(gsub("-","",cohort[cust_idx, 2]), 5, 8),
                     substr(cohort[cust_idx, 2], 1, 4))
  feat_file <- paste0("Feature_df_",feat_dt,".csv")
  
  features[cust_idx] <- arrow::open_dataset(paste0(feat_dir,feat_file),
                                            format = "csv") |>
    select(person_id,
           age,
           <redacted>,
           starts_with(c("previous",
                         "race",
                         "language",
                         "home_county",
                         "coc", # continuity of care measures
                         "<redacted>",
                         "<redacted>"
           ))) |>
    filter(person_id == cohort[cust_idx,1]) |>
    collect() |>
    list()
  
  # print progress message
  if (cust_idx %% 100 == 0) {
    print(paste("Finished row", cust_idx))
  }
  
  # keep time log
  time_log[cust_idx] <- Sys.time()
}

# combine features into a dataframe
features_df <- bind_rows(features)

# how much time did it take to run?
print(paste0("Run time: ", round((time_log[[nrow(cohort)]] - time_log[[1]]) / (60*60),2), " hours"))

# write to s3
s3write_using(features_df,
              write.csv,
              row.names = FALSE,
              bucket = "<redacted>",
              object = "<redacted>/data/features/features_from_scoring.csv"
              )
# Done 
