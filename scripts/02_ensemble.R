
derived_vars <- 
  
  c("days-gte-32C-tasmax",
    "days-gte-35C-tasmax",
    "days-gte-38C-tasmax",
    "ten-hottest-days-tasmax",
    "mean-tasmax",
    "days-lt-0C-tasmax", # 6
    
    "days-gte-20C-tasmin",
    "days-gte-25C-tasmin",
    "mean-tasmin",
    "days-lt-0C-tasmin", # 10
    
    "days-gte-26C-wetbulb",
    "days-gte-28C-wetbulb",
    "days-gte-30C-wetbulb",
    "days-gte-32C-wetbulb",
    "ten-hottest-days-wetbulb", # 15
    
    "mean-tasmean",
    
    "total-precip",
    "ninety-wettest-days",
    # "100yr-storm-precip",
    # "100yr-storm-freq",
    "days-gte-1mm-precip-lt-0C-tasmean",
    "days-gte-b90perc-tasmax-lt-b10perc-precip", #20
    
    "mean-spei",
    "prop-months-lte-neg0.8-spei",
    "prop-months-lte-neg1.6-spei",
    "days-gte-b95perc-fwi",
    
    # "prop-days-gte-b85perc-3dayrunmeantasmin"
    
    "prop-days-gte-b90perc-3dayrunmeantasmax",
    "mean-cont-days-gte-b90perc-3dayrunmeantasmax",
    "mean-tas-gte-b90perc-3dayrunmeantasmax"
    
    )[27] # choose derived variable(s) to assemble



# SETUP -----------------------------------------------------------------------

library(tidyverse)
library(lubridate)
library(stars)
library(furrr)
library(units)

options(future.fork.enable = T)
plan(multicore)

source("scripts/functions.R")

dir_derived <- "/mnt/bucket_mine/results/global_heat_pf/01_derived"
dir_ensembled <- "/mnt/bucket_mine/results/global_heat_pf/02_ensembled"

doms <- c("SEA", "AUS", "CAS", "WAS", "EAS", "AFR", "EUR", "NAM", "CAM", "SAM")

wls <- c("0.5", "1.0", "1.5", "2.0", "2.5", "3.0")

# load thresholds table
thresholds <- 
  str_glue("/mnt/bucket_mine/misc_data/CMIP5_model_temp_thresholds.csv") %>% 
  read_delim() %>%
  suppressMessages() %>% 
  select(1:6) %>% 
  pivot_longer(-Model, names_to = "wl") %>% 
  
  mutate(wl = str_sub(wl, 3)) %>% 
  mutate(wl = ifelse(str_length(wl) == 1, str_glue("{wl}.0"), wl))  %>%
  
  # add institutes
  mutate(Model = case_when(str_detect(Model, "HadGEM") ~ str_glue("MOHC-{Model}"),
                           str_detect(Model, "MPI") ~ str_glue("MPI-M-{Model}"),
                           str_detect(Model, "NorESM") ~ str_glue("NCC-{Model}"),
                           str_detect(Model, "GFDL") ~ str_glue("NOAA-GFDL-{Model}"),
                           str_detect(Model, "MIROC") ~ str_glue("MIROC-{Model}"),
                           TRUE ~ Model))

# load table of variables
tb_vars <-
  read_csv("/mnt/bucket_mine/pf_variable_table.csv") %>% 
  suppressMessages()


# ***************** HEAT WAVES ADDITION!

# tb_vars %>% 
#   rbind(c("minimum_temperature",
#           "prop-days-gte-b85perc-3dayrunmeantasmin",
#           "nothing",
#           "likelihood-heat-wave",
#           "n"
#   )) -> tb_vars

tb_vars %>% 
  rbind(c("maximum_temperature",
          "prop-days-gte-b90perc-3dayrunmeantasmax",
          "nothing",
          "likelihood-heat-wave",
          "n")) %>% 
  rbind(c("maximum_temperature",
          "mean-cont-days-gte-b90perc-3dayrunmeantasmax",
          "nothing",
          "duration-heat-wave",
          "n")) %>% 
  rbind(c("maximum_temperature",
          "mean-tas-gte-b90perc-3dayrunmeantasmax",
          "convert-C",
          "intensity-heat-wave",
          "n")) -> tb_vars


# *****************



# DOMAIN LOOP -----------------------------------------------------------------

for(dom in doms){
  
  print(str_glue(" "))
  print(str_glue("PROCESSING {dom}"))
  
  
  
  # VARIABLE LOOP -------------------------------------------------------------
  
  for(derived_var in derived_vars){
    
    print(str_glue(" "))
    print(str_glue("Processing {derived_var}"))
    
    
    
    ## IMPORT DERIVED VAR FILES -----------------------------------------------
    
    # modifications to imported files
    # e.g. convert units (K ->  C)
    change_import <- 
      tb_vars %>% 
      filter(var_derived == derived_var) %>% 
      pull(change_import)
    
    # vector of files to import
    ff <- 
      dir_derived %>% 
      list.files() %>% 
      str_subset(dom) %>% 
      str_subset(derived_var) %>% 
      
      str_subset("v2")                                                          # ***************************************
    
    
    # import files into a list
    l_s <- 
      
      future_map(ff, function(f){
        
        # apply changes 
        if(change_import == "fix-time+convert-C"){
          read_ncdf(str_glue("{dir_derived}/{f}"), 
                    proxy = F, make_time = F) %>% 
            suppressMessages() %>% 
            suppressWarnings() %>% 
            setNames("v") %>% 
            mutate(v = set_units(v, degC))
          
        } else if(change_import == "fix-time+convert-mm"){
          read_ncdf(str_glue("{dir_derived}/{f}"), 
                    proxy = F, make_time = F) %>% 
            suppressMessages() %>% 
            suppressWarnings() %>% 
            setNames("v") %>% 
            mutate(v = set_units(v, kg/m^2/d))
          
        } else if(change_import == "convert-C"){
          read_ncdf(str_glue("{dir_derived}/{f}"), 
                    proxy = F) %>% 
            suppressMessages() %>% 
            suppressWarnings() %>% 
            setNames("v") %>% 
            mutate(v = set_units(v, degC))
          
        } else if(change_import == "convert-mm"){
          read_ncdf(str_glue("{dir_derived}/{f}"), 
                    proxy = F) %>% 
            suppressMessages() %>% 
            suppressWarnings() %>% 
            setNames("v") %>% 
            mutate(v = set_units(v, kg/m^2/d))
          
        } else if(change_import == "fix-time"){
          read_ncdf(str_glue("{dir_derived}/{f}"), 
                    proxy = F, make_time = F) %>% 
            suppressMessages() %>% 
            suppressWarnings() %>% 
            setNames("v")
          
        } else if(change_import == "nothing"){
          read_ncdf(str_glue("{dir_derived}/{f}"), 
                    proxy = F) %>% 
            suppressMessages() %>% 
            suppressWarnings() %>% 
            setNames("v")
          
        }
        
        
      },
      .options = furrr_options(seed = NULL)) %>% 
      
      # fix time dim
      map(function(s){
        
        s %>%
          st_set_dimensions("time",
                            values = st_get_dimension_values(s, "time") %>%
                              as.character() %>%
                              str_sub(end = 4) %>% 
                              as.integer()) %>%
          #   drop_units()
          
          # s %>%
          #   st_set_dimensions("time",
          #                     values = st_get_dimension_values(s, "time") %>%
          #                       # ymd() %>%
          #                       year()
          #                     ) %>%
          
          # s %>%
          #   st_set_dimensions("time",
        #                     values = seq(1970, length.out = dim(s)[3])
        #   ) %>%
        mutate(v = set_units(v, NULL))
      })
    
    
    # Verify correct import
    print(str_glue("Imported:"))
    
    walk2(l_s, ff, function(s, f){
      
      yrs <-
        s %>% 
        st_get_dimension_values("time")
      
      range_time <- 
        yrs %>% 
        range()
      
      time_steps <- 
        yrs %>% 
        length()
      
      mod <- 
        f %>% 
        str_extract("(?<=yr_)[:alnum:]*_[:graph:]*(?=\\.nc)")
      
      print(str_glue("   {mod}: \t{range_time[1]} - {range_time[2]} ({time_steps} timesteps)"))
      
    })
    
    
    ## 
    
    size_rasters <- l_s %>% map(dim) %>% map_int(~.x[1]*.x[2])
    
    if(size_rasters %>% unique() %>% length() %>% {. > 1}){
      
      print("   ...warping...")
      
      smallest <- size_rasters %>% which.min()
      
      larger <- which(size_rasters != size_rasters[smallest])
      
      l_s[larger] <- 
        
        l_s[larger] %>% 
        map(function(s){
          
          st_warp(s, l_s[[smallest]] %>% slice(time, 1))
          
        })
      
    }
    
    
    
    ## 
    if(str_detect(derived_var, "prop-months")){
      
      print("...masking drought layers...")
      
      l_s <- 
        
        map2(ff, l_s, function(f, s){
          
          # extract GCM to identify threshold year
          gcm_ <- 
            f %>% 
            str_split("_", simplify = T) %>% 
            .[,5] %>% 
            str_remove(".nc")
          
          # extract GCM to identify threshold year
          rcm_ <- 
            f %>% 
            str_split("_", simplify = T) %>% 
            .[,4] %>% 
            str_remove(".nc")
          
          # mask
          if(rcm_ == "REMO2015"){
            d <- str_glue("/mnt/bucket_cmip5/RCM_regridded_data/REMO2015/{dom}/monthly/spei/")
          } else {
            d <- str_glue("/mnt/bucket_cmip5/RCM_regridded_data/CORDEX_22/{dom}/monthly/spei/")
          }
          
          
          e <- "try-error"
          
          while(e == "try-error"){
            
            mask <- 
              try(
                d %>% 
                  list.files(full.names = T) %>% 
                  str_subset(gcm_) %>% 
                  first() %>% 
                  read_ncdf(ncsub = cbind(start = c(1,1,20),
                                          count = c(NA, NA, 1))) %>% 
                  suppressMessages() %>% 
                  suppressWarnings() %>% 
                  adrop()
              )
            
            e <- class(mask)
            
            Sys.sleep(2)
            
          }
          
          
          # apply mask
          s[is.na(mask)] <- NA
          
          return(s)
          
        })
      
    }
    
    
    
    
    
    ## SLICE BY WARMING LEVELS ------------------------------------------------
    
    l_s_wl <- 
      
      # loop through warming levels
      map(wls, function(wl){
        
        print(str_glue("Slicing WL {wl}"))
        
        # loop through models
        map2(ff, l_s, function(f, s){
          
          # rcm_ <- f %>% str_split("_", simplify = T) %>% .[,4]
          
          # extract GCM to identify threshold year
          gcm_ <- 
            f %>% 
            str_split("_", simplify = T) %>% 
            .[,5] %>% 
            str_remove(".nc")
          
          # baseline:
          if(wl == "0.5"){
            
            s %>% 
              filter(time >= 1971,
                     time <= 2000)
            
          # other warming levels:
          } else {
            
            thres_val <-
              thresholds %>%
              filter(str_detect(Model, str_glue("{gcm_}$"))) %>% 
              filter(wl == {{wl}})
            
            s <- 
              s %>% 
              filter(time >= thres_val$value - 10,
                     time <= thres_val$value + 10)
            
            # verify correct slicing:
            print(str_glue("   {gcm_}: {thres_val$Model}: {thres_val$value}"))
            
            return(s)
          }
          
        }) %>% 
          
          # concatenate all models and form a single time dimension
          {do.call(c, c(., along = "time"))}
        
      })
    
    
    ## CALCULATE STATISTICS ---------------------------------------------------
    
    # Statistics are calculated per grid cell (across time dimension). They
    # include the mean, the median, and the 5th and 95th percentile.
    
    l_s_wl_stats <-
      
      # loop through warming levels
      imap(wls, function(wl, iwl){
        
        print(str_glue("Calculating stats WL {wl}"))
        
        l_s_wl %>%
          pluck(iwl) %>%
          
          st_apply(c(1,2), function(ts){
            
            # if a given grid cell is empty, propagate NAs
            # if(any(is.na(ts))){
            ### fix for heatwave intensity: 
            if(all(is.na(ts))){
              
              c(mean = NA,
                perc05 = NA, 
                perc50 = NA,
                perc95 = NA)
              
            } else {
              
              # c(mean = mean(ts),
              #   quantile(ts, c(0.05, 0.5, 0.95)) %>% 
              #     setNames(c("perc05", "perc50", "perc95")))
              
              c(mean = mean(ts, na.rm = T),
                quantile(ts, c(0.05, 0.5, 0.95), na.rm = T) %>% 
                  setNames(c("perc05", "perc50", "perc95")))
              
            }
            
          },
          FUTURE = T,
          .fname = "stats") %>%
          aperm(c(2,3,1)) %>%
          split("stats")
        
      })
    
    
    # concatenate warming levels
    s_result <-
      l_s_wl_stats %>%
      {do.call(c, c(., along = "wl"))} %>%
      st_set_dimensions(3, values = as.numeric(wls))
    
    
    
    
    ## SAVE RESULT ------------------------------------------------------------
    
    print(str_glue("Saving result"))
    
    res_filename <- 
      str_glue(
        "{dir_ensembled}/{dom}_{derived_var}_ensemble.nc"
      )
    
    fn_write_nc(s_result, res_filename, "wl")
    
    
  }
  
}

