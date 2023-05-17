# Train Weight Status Models ---------------------------------------------------
## Adult and Pediatric Models trained in sequence, 
## using predictions about adults in the household (HH) to improve pediatric predictions
## Outcome: binary outcome proven to be best for both adult and pediatric

# Setup ----
## Libraries
source(here::here("code/common/setup.R"))
library(h2o)

# customer functions
source("<redacted>/functions/experiment_track.R")

## Load Data
features <- s3read_using(
  arrow::read_parquet,
  bucket = "<redacted>",
  object = "<redacted>/data/dev/features/refined_featureset.parquet"
)

target_train <- s3read_using(
  read.csv,
  bucket = "<redacted>",
  object = "<redacted>/data/dev/target/training_cohort_and_labels.csv"
) |>
  left_join(features,
            by = "<redacted>")

target_test <- s3read_using(
  read.csv,
  bucket = "<redacted>",
  object = "<redacted>/data/dev/target/testing_cohort_and_labels.csv"
) |>
  left_join(features,
            by = "<redacted>")


## Start h2o 
h2o.init(max_mem_size = "40G")


## Feature Set ----
### Selected through theory/literature, SME discussions, and iterative feature removal
feat_vec <- c(
  "age",
  "female",
  "citizen",
  "<redacted>",
  "<redacted>",
  "<redacted>",
  "<redacted>",
  "county_<redacted>",
  "county_<redacted>",
  "county_<redacted>",
  "county_<redacted>",
  "county_<redacted>",
  "county_<redacted>",
  "county_<redacted>",
  "county_<redacted>",
  "county_<redacted>",
  "plan_type_<redacted>",
  colnames(select(target_train, starts_with("mcd_"))),
  colnames(select(target_train, starts_with("chp_"))),
  colnames(select(target_train, starts_with("ep_"))),
  colnames(select(target_train, starts_with("mcr_"))),
  colnames(select(target_train, starts_with("cc_"))),
  "mobility_issue",
  "lang_english",
  "lang_spanish",
  "lang_mena",
  "lang_pcfisl",
  "lang_seasian",
  "lang_sasian",
  "lang_easia",
  "lang_euro",
  "lang_creole",
  "lang_african",
  "re_aian_k",
  "re_asian_pi_k",
  "re_black_k",
  "re_latino_k",
  "re_other_k",
  "re_white_k",
  "re_asian_pi_m",
  "re_afr_carib_m",
  "re_euro_m",
  "re_aian_m",
  "re_mena_m",
  "re_latino_m",
  "diabetes",
  "prediabetes",
  "hyperten_comb",
  "circulatory",
  "hyperlipid",
  "asthma",
  "nsaids",
  "skin_cond",
  "nutr_sup",
  "macro_nutri_defic",
  "micro_nutri_defic",
  "vit_d_defic",
  "mood_rx",
  "ulcer_rx",
  "nasal_rx",
  "cdc_obese_pct",
  "homeless",
  "hh_obese",
  "hh_nonobese",
  "ip","er","pcp","uc","specialist",
  "n_diff_drugs"
)


# Adults -----------------------------------------------------------------------
## Adult-Specific Features ----
### Remove those for pediatrics only
adult_feat_rm <- c("mcd_foster",   # plans that adults cannot have
                   "chp_fpl_<redacted>"
)

feat_vec_adults <- feat_vec[!feat_vec %in% adult_feat_rm]


## Adult Data ----

### do age restriction
### create weights (best balancing method found in prior experimentation)
adult_train <- target_train |>
  filter(age %in% 20:110) |>
  mutate(
    adult_weights = 
      case_when(
        obese == 1 ~ nrow(filter(target_train, 
                                 age %in% 20:110, 
                                 obese == 0))/nrow(filter(target_train, 
                                                          age %in% 20:110,
                                                          obese == 1)),
        obese == 0 ~ 1)) |>
  as.h2o()

adult_test <- target_test |>
  filter(age %in% 20:110) |>
  mutate(
    adult_weights = 
      case_when(
        obese == 1 ~ nrow(filter(target_test, 
                                 age %in% 20:110, 
                                 obese == 0))/nrow(filter(target_test, 
                                                          age %in% 20:110,
                                                          obese == 1)),
        obese == 0 ~ 1)) |>
  as.h2o()

## Adult Model ----

### automl has done a great job
m_adult <- h2o.automl(y = "obese",
                      training_frame = adult_train,
                      x = feat_vec_adults,
                      max_models = 50,
                      # max_runtime_secs = 60*30,
                      nfolds = 10,
                      seed = 789,
                      weights_column = "adult_weights",
                      include_algos = c("XGBoost","GBM","GLM"),
                      stopping_metric = "logloss",
                      stopping_tolerance = 0.0001,
                      stopping_rounds = 5)

# check metrics for best models
h2o::h2o.get_leaderboard(m_adult)

# record experiment (artifacts, metadata, and metrics) 
experiment_track(m_adult,
                 path = "s3://<redacted>/experiment_tracking/adults",
                 test_frame = adult_test)

## Save Adult Model ----
### save to S3
h2o.saveModel(object = m_adult@leader,
              path = "s3a://<redacted>/models/adult_models",
              force = TRUE,
              export_cross_validation_predictions = TRUE)
h2o.save_mojo(object = m_adult@leader,
              path = "s3a://<redacted>/models/adult_models",
              force = TRUE)



# Pediatric --------------------------------------------------------------------

## Pediatric-Specific Features ----
ped_feat_rm <- c("mcd_<redacted>",   # plans that peds don't have
                 "mcr_<redacted>",
                 "mcr_<redacted>",
                 "mcr_<redacted>",
                 "cc_<redacted>",
                 "cc_<redacted>"
)

feat_vec_peds <- c(feat_vec[!feat_vec %in% ped_feat_rm], "hh_adult_mean_score")


## Pediatric Data ----

# produce adult scores, find hh mean, and use as feature in ped model
# Household IDs
hh_ids <- tbl(con_access,
              sql(read_file(here("<redacted>/hh_ids.sql")))) |> # get cust and hh ids for scoring popn
  collect()

# use the current model to score adults
source("<redacted>/score_adults.R")

# load adult scores and get household mean (performs better than max)
hh_adult_scores <- 
  s3read_using(read.csv,
               bucket = "<redacted>",
               object = paste0("<redacted>/scores/adult_scores_",Sys.Date(),".csv")) |>
  select(cust_id, p1) |>
  left_join(hh_ids, by = "cust_id") |>
  group_by(hh_id) |>
  summarize(hh_adult_mean_score = mean(p1, na.rm = TRUE))


### weights
### age restiction
### join adult scores
ped_train <- target_train |>
  filter(age %in% 2:19) |>
  left_join(hh_adult_scores, by = "hh_id") |>
  mutate(
    ped_weights = 
      case_when(obese == 1 ~ nrow(filter(target_train, 
                                         age %in% 2:19,
                                         obese == 0))/nrow(filter(target_train,
                                                                  age %in% 2:19, 
                                                                  obese == 1)),
                obese == 0 ~ 1)) |>
  as.h2o()

ped_test <- target_test |>
  filter(age %in% 2:19) |>
  left_join(hh_adult_scores, by = "hh_id") |>
  mutate(
    ped_weights = 
      case_when(obese == 1 ~ nrow(filter(target_test, 
                                         age %in% 2:19,
                                         obese == 0))/nrow(filter(target_test,
                                                                  age %in% 2:19, 
                                                                  obese == 1)),
                obese == 0 ~ 1)) |>
  as.h2o()

## Pediatric Model ----
m_ped <- h2o.automl(y = "obese",
                    training_frame = ped_train,
                    x = feat_vec_peds,
                    max_models = 50,
                    # max_runtime_secs = 60*30,
                    nfolds = 10,
                    seed = 789,
                    weights_column = "ped_weights",
                    include_algos = c("XGBoost","GBM","GLM"),
                    stopping_metric = "logloss",
                    stopping_tolerance = 0.0001,
                    stopping_rounds = 5)

# check metrics for best models
h2o::h2o.get_leaderboard(m_ped)

# record experiment (artifacts, metadata, and metrics) 
experiment_track(m_ped,
                 path = "s3://<redacted>/experiment_tracking/peds",
                 test_frame = ped_test)


## Save Ped Model ----
### save to S3
h2o.saveModel(object = m_ped@leader,
              path = "s3a://<redacted>/ped_models",
              force = TRUE,
              export_cross_validation_predictions = TRUE)
h2o.save_mojo(object = m_ped@leader,
              path = "s3a://<redacted>/ped_models",
              force = TRUE)

# Stop h2o cluster
h2o.shutdown()

# Done
