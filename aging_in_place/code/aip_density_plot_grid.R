# Make density plots of census tract-level data with NY Overall overlay

# Setup ------------------------------------------------------------------------
## Libraries
library(tidyverse)
library(aws.s3)
library(here)

## Data 
# Run and load individual level AIP cohort data
source(here("<redacted>/prepare_data_for_analysis.R"))

# specify the fields we want to plot 
flist_neighborhood <- c(
  "ds_pm_pred",   # air pollution
  "perc_black",   # % black
  "perc_hisp",    # % hispanic
  "perc_color",   # % people of color
  "ice_wb",       # income concentration white/black
  "ice_wbh",      # income concentration white/black/hispanic
  "poverty",      # % in poverty
  "crowding",     # % of households that are "crowded"
  "per_noinsure", # % uninsured
  "bach_or_more", # % of adults (ages 25+) with bachelor's degree or higher
  "per_unemply",  # % unemployed
  "pop_total"     # total population in census tract
)

# Load tract-level data from partners and average values for both years (2017-2018) ----
ny_overall <- s3read_using(readxl::read_excel,
                           bucket = "<redacted>",
                           object = "<redacted>/predisposing/Community_level_vars_ny_subset_pm25_2016.xlsx",
                           sheet = "data") |>
  janitor::clean_names() |>
  # fix some names
  rename(ice_wb_2017 = ic_ewbinc_2017,
         ice_wb_2018 = ic_ewbinc_2018,
         ice_wbh_2017 = ic_ewnhinc_2017,
         ice_wbh_2018 = ic_ewnhinc_2018) |>
  #pivot longer to more easily average data from the two years
  pivot_longer(cols = c(ends_with("2017"), ends_with("2018")),
               names_to = c(".value","year"),
               names_pattern = "(.*)_(\\d+)") |>
  # remove tracts with 0 population
  filter(pop_total != 0) |>
  # get rid of some variables
  select(-c(ctfips, x24, latitude, longitude, year)) |>
  # group and carry some fields along
  group_by(geoid, ds_pm_pred) |>
  # calculate the mean of the rest for the two years
  summarize(across(everything(), ~ mean(., na.rm = TRUE)),
            .groups = "keep") |>
  ungroup() |>
  # make grouping variable for plotting
  mutate(grouping_var = "NY Overall") |>
  # keep only fields of interest
  select(all_of(flist_neighborhood),
         grouping_var)

# class(aip$ltc_cat_simple)

# Plot -------------------------------------------------------------------------
aip |>
  # keep only those who have geographic data in the lookback window
  inner_join(cust_geo_in_window, by = "cust_id") |>
  # exclude those whose tracts have no data
  filter(!is.na(ice_wb)) |>
  # harmonize the group variable name with that made above
  rename(grouping_var = ltc_cat_simple) |>
  # keep only the continuous variables + the label
  select(all_of(flist_neighborhood),
         grouping_var) |>
  # since grouping_var is factor, make character for now to include new category after row bind
  mutate(grouping_var = as.character(grouping_var)) |>
  # add the NY Overall data
  bind_rows(ny_overall) |>
  # there actually is variation in pop total, so use for weighting tract-level data
  mutate(weight_tract = ifelse(grouping_var == "NY Overall", pop_total, 1)) |>
  # prepare for ggplot-ing and faceting
  pivot_longer(cols = -c(grouping_var, weight_tract),
               names_to = "variables",
               values_to = "values") |> 
  # plot
  ggplot() +
  geom_density(aes(x = values,
                   fill = grouping_var,
                   color = grouping_var,
                   weight = weight_tract),
               alpha = .15) +
  facet_wrap(vars(variables),
             scales = "free",
             ncol = 3) +
  scale_fill_brewer(palette = 6,
                    type = "qual",
                    aesthetics = c("color","fill")) +
  theme_bw() +
  # theme(panel.background = element_blank(),
  #       panel.border = element_rect(color = "grey80", fill = NA)) +
  theme(plot.caption.position = "plot",
        plot.caption = element_text(hjust = 0),
        plot.title.position = "plot") +
  labs(title = "Density of Tract-Level Data Over Members in Study By Study Category With NY Overall Density",
       fill = "",
       color = "",
       caption = paste("The unit of analysis for each LTC group is the",
                       "individual member with tract level data joined,",
                       "so tracks are represented many times in the\nindividual",
                       "level data. Since NY Overall uses census tracts as the",
                       "units of analysis, those data are weighted by the total",
                       "population in each\ntract to make the densities comparable."))

ggsave(filename = "community_feature_density_ltc_to_ny_overall.png",
       path = "output/figures",
       device = "png",
       height = 7,
       width = 9)
