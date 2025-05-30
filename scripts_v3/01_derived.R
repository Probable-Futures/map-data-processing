

# CHOOSE VARIABLE(S) TO PROCESS
# see table "scripts/tb_vars_all.R" to get id 
var_index <- c(4)



# SETUP -----------------------------------------------------------------------

library(tidyverse)
library(lubridate)
library(stars)
library(furrr)
library(units)

options(future.fork.enable = T)
plan(multicore)


source("scripts/setup.R") # load main directory routes 
source("scripts/fn_derived.R") # load main function to calculate derived vars
source("scripts/functions.R") # other functions


dir_derived <- str_glue("{dir_results}/01_derived")

doms <- c("SEA", "AUS", "CAS", "WAS", "EAS", "AFR", "EUR", "NAM", "CAM", "SAM")

# load table of all variables
source("scripts/tb_vars_all.R")

# subset those that will be processed
tb_vars <- 
  tb_vars_all[var_index, ]




# PROCESS DERIVED VARIABLES ---------------------------------------------------


# INPUT VARIABLE LOOP -----

vars_input <- 
  tb_vars %>% 
  pull(var_input) %>% 
  unique()

for (var_input in vars_input) {
  
  print(str_glue(" "))
  print(str_glue("PROCESSING VARS BASED ON {var_input}"))
  
  
  # DOMAIN LOOP -----
  
  for(dom in doms){
    
    print(str_glue(" "))
    print(str_glue("PROCESSING {dom}"))
    
    # assemble table of needed files for calculation
    tb_files <-
      str_split(var_input, "[+]") %>% .[[1]] %>% # in case > 1 variable 
      map_dfr(fn_data_table) %>% 
      
      filter(str_detect(file, "MISSING", negate = T))
    
    # extract models
    tb_models <-
      unique(tb_files[, c("gcm", "rcm")]) %>% 
      arrange(rcm, gcm)
    
    # ignore RegCM in these domains  
    if(dom %in% c("SAM", "AUS", "CAS")){
      
      tb_models <- 
        tb_models %>% 
        filter(str_detect(rcm, "RegCM", negate = T))
      
    }
    
    
    
    # MODEL LOOP ------
    
    for(i in seq_len(nrow(tb_models))){
      
      print(str_glue(" "))
      print(str_glue("PROCESSING model {i} / {nrow(tb_models)}"))
      
      gcm_ <- tb_models$gcm[i]
      rcm_ <- tb_models$rcm[i]
      
      
      ## DOWNLOAD RAW DATA -----------------------------------------------------
      
      print(str_glue("Downloading raw data [{rcm_}] [{gcm_}]"))
      
      dir_raw_data <- str_glue("{dir_disk}/raw_data")
      
      if(dir.exists(dir_raw_data)){
        print(str_glue("   (previous dir_raw_data deleted)"))
        unlink(dir_raw_data, recursive = T)
      } 
      
      dir.create(dir_raw_data)
      
      # table of files for 1 model
      tb_files_mod <- 
        tb_files %>% 
        filter(gcm == gcm_,
               rcm == rcm_) 
      
      # download files in parallel  
      tb_files_mod %>% 
        future_pwalk(function(loc, file, ...){
          
          loc_ <-
            loc %>% 
            str_replace("/mnt/bucket_cmip5", "gs://cmip5_data")
          
          str_glue("{loc_}/{file}") %>%
            {system(str_glue("gsutil cp {.} {dir_raw_data}"), 
                    ignore.stdout = T, ignore.stderr = T)}
          
        })
      
      "   Done: {length(list.files(dir_raw_data))} / {nrow(tb_files_mod)} files downloaded" %>% 
        str_glue() %>% 
        print()
      
      
      
      # FWI raw layers from REMO need the time dimension to be properly formatted 
      
      if(str_detect(var_input, "fire") & str_detect(rcm_, "REMO")){
        
        future_pwalk(tb_files_mod, function(file, t_i, ...){
          
          infile <- str_glue("{dir_raw_data}/{file}")
          outfile <- infile %>% str_replace(".nc", "_fixed_time.nc")
          
          "cdo -setcalendar,365_day -settaxis,{as.character(ymd(t_i))},12:00:00,1day {infile} {outfile}" %>% 
            str_glue %>% 
            system(ignore.stdout = T, ignore.stderr = T)
          
          file.remove(infile)
          
          file.rename(outfile, infile)
          
        })
        
      }
      
      
      
      ## CDO PRE-PROCESS -------------------------------------------------------
      
      # CDO is used here to split files annually and fix their time dimension.
      # This process solves some inconsistencies present in RegCM4 models.
      # Then files are concatenated again into one single file with the complete
      # record of years (~ 1970-2100). Concatenating everything into one file 
      # eases successive calculations to obtain derived variables.
      
      
      print(str_glue("Pre-processing with CDO [{rcm_}] [{gcm_}]"))
      
      tb_files_mod %>%
        future_pwalk(function(file, t_i, t_f, ...){
          
          # extract first and last year included in the file
          yr_i <- year(as_date(t_i))
          yr_f <- year(as_date(t_f))
          
          f <- str_glue("{dir_raw_data}/{file}")
          
          
          if(str_detect(var_input, "wetbulb")){ # already split annually
            
            f_new <- str_glue("{dir_raw_data}/yrfix_{yr_i}.nc")
            
            # fix time
            system(str_glue("cdo -a setdate,{yr_i}-01-01 {f} {f_new}"),
                   ignore.stdout = T, ignore.stderr = T)
            
            file.remove(f)
            
            
          } else {
            
            # extract variable's (short) name
            v <- str_split(file, "_") %>% .[[1]] %>% .[1]
            
            # split annually
            system(str_glue("cdo splityear {f} {dir_raw_data}/{v}_yrsplit_"),
                   ignore.stdout = T, ignore.stderr = T)
            
            # fix time (only of the files that came from the file above)
            dir_raw_data %>% 
              list.files(full.names = T) %>% 
              str_subset(v) %>% 
              str_subset("yrsplit") %>%
              str_subset(str_flatten(yr_i:yr_f, "|")) %>% 
              
              walk2(seq(yr_i, yr_f), function(f2, yr) {
                
                f_new <- str_glue("{dir_raw_data}/{v}_yrfix_{yr}.nc")
                
                system(str_glue("cdo -a setdate,{yr}-01-01 {f2} {f_new}"),
                       ignore.stdout = T, ignore.stderr = T)
                
                file.remove(f2)
                
              })
            
            file.remove(f)
            
          }
          
          
        })
      
      
      # check if some files could not be year-split/time-fixed
      bad_remnants <- 
        dir_raw_data %>% 
        list.files(full.names = T) %>% 
        str_subset("yrsplit")
      
      if (length(bad_remnants) > 0) {
        print(str_glue("   ({length(bad_remnants)} bad file(s) - deleted)"))
        
        bad_remnants %>% 
          walk(file.remove)
        
      }
      
      
      # concatenate
      
      dir_cat <- str_glue("{dir_disk}/cat")
      
      if (dir.exists(dir_cat)) {
        print(str_glue("   (previous dir_cat deleted)"))
        unlink(dir_cat, recursive = T)
      }
      
      dir.create(dir_cat)
      
      # extract variable(s)'s (short) name
      v <- 
        dir_raw_data %>%
        list.files() %>% 
        str_split("_", simplify = T) %>% 
        .[,1] %>% 
        unique()
      
      # loop through variable(s)
      future_walk(v, function(vv){
        
        ff <-
          dir_raw_data %>%
          list.files(full.names = T) %>%
          str_subset(str_glue("{vv}_")) %>% 
          str_flatten(" ")
        
        system(str_glue("cdo -settunits,d  -cat {ff} {dir_cat}/{vv}_cat.nc"),
               ignore.stdout = T, ignore.stderr = T)
        
      })
      
      
      
      ## CALCULATE DERIVED VARIABLES ------------------------------------------
      
      # This section calculates derived variables from the concatenated file
      # (some vars use the annual files at dir_raw_data).
      # Derived variables that use the same input variable are calculated 
      # at once. This step uses function "fn_derived" from the "fn_derived.R" 
      # script loaded in the SETUP section above. Refer to it to see how each 
      # derived variable is obtained.
      
      
      print(str_glue("Calculating derived variables [{rcm_}] [{gcm_}]"))
      
      # identify all vars to derive from input var
      tb_derived_vars <-
        tb_vars %>%
        filter(var_input == {{var_input}})
      
      # process variables not eligible for parallelization
      tb_derived_vars_np <-
        tb_derived_vars %>%
        filter(parallel == "n")
      
      if (nrow(tb_derived_vars_np) > 0) {
        pwalk(tb_derived_vars_np, function(var_derived, ...){
          fn_derived(var_derived)
        })
      }
      
      # process variables eligible for parallelization
      tb_derived_vars_p <- 
        tb_derived_vars %>% 
        filter(parallel == "y")
      
      if (nrow(tb_derived_vars_p) > 0) {
        future_pwalk(tb_derived_vars_p, function(var_derived, ...){
          fn_derived(var_derived)
        })
      }
      
      # delete concatenated file
      unlink(dir_cat, recursive = T)
      
      # delete raw files
      unlink(dir_raw_data, recursive = T)
      
    }
    
  }
  
}



