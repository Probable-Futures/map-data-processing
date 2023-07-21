
library(tidyverse)
library(lubridate)
library(stars)
library(furrr)
library(units)

options(future.fork.enable = T)


dir_bucket_cmip5 <- "/mnt/bucket_cmip5"
dir_bucket_mine <- "/mnt/bucket_mine"
dir_pers_disk <- "/mnt/pers_disk"



# THRESHOLD TABLE ---------------------------------------------------------------------------------

thresholds <- 
  
  str_glue("{dir_bucket_mine}/misc_data/CMIP5_model_temp_thresholds.csv") %>% 
  read_delim() %>%
  suppressMessages() %>% 
  select(1:6) %>% 
  pivot_longer(-Model, names_to = "wl") %>% 
  mutate(wl = str_sub(wl, 3)) %>% 
  
  mutate(wl = ifelse(str_length(wl) == 1, str_glue("{wl}.0"), wl))  %>%
  
  mutate(Model = case_when(str_detect(Model, "HadGEM") ~ str_glue("MOHC-{Model}"),
                           str_detect(Model, "MPI") ~ str_glue("MPI-M-{Model}"),
                           str_detect(Model, "NorESM") ~ str_glue("NCC-{Model}"),
                           str_detect(Model, "GFDL") ~ str_glue("NOAA-GFDL-{Model}"),
                           str_detect(Model, "MIROC") ~ str_glue("MIROC-{Model}"),
                           TRUE ~ Model))



# VARIABLE TABLE ----------------------------------------------------------------------------------

tb_vars <- 
  
  read_csv("/mnt/bucket_mine/pf_variable_table.csv") %>% 
  suppressMessages()


# rbind(tb_vars, c("spei", "months-lte-neg1.6-spei", "nothing", "likelihood-yearplus-extreme-drought")) -> tb_vars
# 
# tb_vars %>%
#   write_csv("/mnt/bucket_mine/pf_variable_table.csv")

# tb_vars %>% mutate(parallel = c("y", "y", "y", "n", "y", "y","y", "y", "y", "y","y", "y", "y", "y", "n", "y", "y", "n", "-", "-","-", "-", "y", "-", "-")) -> tb_vars

# tb_vars <- 
#   tibble(
#     var_derived = c("days-gec-32C-tasmax",
#                     "days-gec-35C-tasmax",
#                     "days-gec-38C-tasmax",
#                     "ten-hottest-days-tasmax",
#                     "mean-tasmax",
#                     "days-lec-0C-tasmax",
#                     
#                     "days-gec-20C-tasmin",
#                     "days-gec-25C-tasmin",
#                     "mean-tasmin",
#                     "days-lec-0C-tasmin",
#                     
#                     "days-gec-26C-wetbulb",
#                     "days-gec-28C-wetbulb",
#                     "days-gec-30C-wetbulb",
#                     "days-gec-32C-wetbulb",
#                     "ten-hottest-days-wetbulb",
#                     
#                     "mean-tasmean",
#                     
#                     # *****************
#                     
#                     "total-precip",
#                     "ninety-wettest-days",
#                     "-",
#                     "-",
#                     "days-gec-1mm-precip-lec-0C-tas",
#                     "days-ltc-b10thperc-precip-gec-b90thperc-tasmax"
#     ),
#     
#     
#     var_input = c("maximum_temperature",
#                   "maximum_temperature",
#                   "maximum_temperature",
#                   "maximum_temperature",
#                   "maximum_temperature",
#                   "maximum_temperature",
#                   
#                   "minimum_temperature",
#                   "minimum_temperature",
#                   "minimum_temperature",
#                   "minimum_temperature",
#                   
#                   "maximum_wetbulb_temperature",
#                   "maximum_wetbulb_temperature",
#                   "maximum_wetbulb_temperature",
#                   "maximum_wetbulb_temperature",
#                   "maximum_wetbulb_temperature",
#                   
#                   "average_temperature",
#                   
#                   # *******************
#                   
#                   "precipitation",
#                   "precipitation",
#                   "precipitation",
#                   "precipitation",
#                   "precipitation+average_temperature",
#                   "precipitation+maximum_temperature"
#                   
#     ),
#     
#     change_import = c("fix_date",
#                       "fix_date",
#                       "fix_date",
#                       "fix_date_convert_C",
#                       "fix_date_convert_C",
#                       "fix_date",
#                       
#                       "fix_date",
#                       "fix_date",
#                       "fix_date_convert_C",
#                       "fix_date",
#                       
#                       "fix_date",
#                       "fix_date",
#                       "fix_date",
#                       "fix_date",
#                       "fix_date", # already in C
#                       
#                       "fix_date_convert_C",
#                       
#                       # ***************
#                       
#                       "convert_mm",
#                       "convert_mm",
#                       "-",
#                       "-",
#                       "nothing",
#                       "nothing"
#                       
#     ),
#     
#     final_name = c("days-above-32C",
#                    "days-above-35C",
#                    "days-above-38C",
#                    "ten-hottest-days",
#                    "average-daytime-temperature",
#                    "freezing-days",
#                    
#                    "nights-above-20C",
#                    "nights-above-25C",
#                    "average-nighttime-temperature",
#                    "frost-nights",
#                    
#                    "days-above-26C-wetbulb",
#                    "days-above-28C-wetbulb",
#                    "days-above-30C-wetbulb",
#                    "days-above-32C-wetbulb",
#                    "ten-hottest-wetbulb-days",
#                    
#                    "average-temperature",
#                    
#                    # ******************
#                    
#                    "change-total-annual-precipitation",
#                    "change-90-wettest-days",
#                    "change-100yr-storm-precip",
#                    "change-100yr-storm-freq",
#                    "change-snowy-days",
#                    "change-dry-hot-days"
#       
#     )
#   )

  