
var_input <- 
  
  c(# heat module
    "maximum_temperature",
    "minimum_temperature",
    "maximum_wetbulb_temperature",
    "average_temperature",
    
    # water module
    "precipitation",
    "precipitation+average_temperature",
    "precipitation+maximum_temperature",
    
    # land module
    "spei",
    "fire_weather_index")[1] # choose input variable to process




# SETUP -----------------------------------------------------------------------

library(tidyverse)
library(lubridate)
library(stars)
library(furrr)
library(units)

options(future.fork.enable = T)
plan(multicore)


# load main function to calculate derived vars
source("scripts/fn_derived.R") 
source("scripts/functions.R") # other functions


dir_cordex <- "/mnt/bucket_cmip5/RCM_regridded_data"
dir_derived <- "/mnt/bucket_mine/results/global_heat_pf/01_derived"
dir_tmp <- "/mnt/pers_disk"

doms <- c("SEA", "AUS", "CAS", "WAS", "EAS", "AFR", "EUR", "NAM", "CAM", "SAM")

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
          "nothing",
          "intensity-heat-wave",
          "n")) -> tb_vars


# *****************



# DOMAIN LOOP -----------------------------------------------------------------

for(dom in doms){                                                                 # *******************
  
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
  
  
  
  # MODEL LOOP ----------------------------------------------------------------
  
  # *********
  
  # i = which(str_detect(tb_models$gcm[which(str_detect(tb_models$rcm, "REMO"))], "Had"))
  for(i in 1:3) {                                                                # *****************************
  
  # *********
  
  
  
  # for(i in seq_len(nrow(tb_models))){
    
    print(str_glue(" "))
    print(str_glue("PROCESSING model {i} / {nrow(tb_models)}"))
    
    gcm_ <- tb_models$gcm[i]
    rcm_ <- tb_models$rcm[i]
    
    
    ## DOWNLOAD RAW DATA ------------------------------------------------------
    
    print(str_glue("Downloading raw data [{rcm_}] [{gcm_}]"))
    
    dir_raw_data <- str_glue("{dir_tmp}/raw_data")
    
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
    
    
    
    ## 
    
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
    
    
    
    ## CDO PRE-PROCESS --------------------------------------------------------
    
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
            
            walk2(seq(yr_i, yr_f), function(f2, yr){
              
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
    
    if(length(bad_remnants) > 0){
      print(str_glue("   ({length(bad_remnants)} bad file(s) - deleted)"))
      
      bad_remnants %>% 
        walk(file.remove)
    }
    
    
    # concatenate
    
    dir_cat <- str_glue("{dir_tmp}/cat")
    
    if(dir.exists(dir_cat)){
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
    
    
    
    ## CALCULATE DERIVED VARIABLES --------------------------------------------
    
    # This section calculates derived variables from the concatenated file.
    # All derived variables that use the same input variable are calculated 
    # at once. This step uses function "fn_derived" from the "fn_derived.R" 
    # file loaded in the SETUP section above. Refer to it to see how each 
    # derived variable was obtained.
    
    
    print(str_glue("Calculating derived variables [{rcm_}] [{gcm_}]"))
    
    # # identify all vars to derive from input var
    # tb_derived_vars <-
    #   tb_vars %>%
    #   filter(var_input == {{var_input}})
    # 
    # # process variables not eligible for parallelization
    # tb_derived_vars_np <-
    #   tb_derived_vars %>%
    #   filter(parallel == "n")
    # 
    # if(nrow(tb_derived_vars_np) > 0){
    #   pwalk(tb_derived_vars_np, function(var_derived, ...){
    #     fn_derived(var_derived)
    #   })
    # }
    # 
    # # process variables eligible for parallelization
    # tb_derived_vars_p <- 
    #   tb_derived_vars %>% 
    #   filter(parallel == "y")
    # 
    # if(nrow(tb_derived_vars_p) > 0){
    #   future_pwalk(tb_derived_vars_p, function(var_derived, ...){
    #     fn_derived(var_derived)
    #   })
    # }

    
        
    # derived_var <- tb_vars$var_derived[29]
    # 
    # outfile <-
    #   str_glue("{dir_derived}/{dom}_{derived_var}_yr_{rcm_}_{gcm_}.nc")
    
    
    
    # select 90 hottest days per year
    dir_raw_data %>%
      list.files() %>%
      str_subset(seq(1971,2000) %>% str_flatten("|")) %>% 
      future_walk(function(f){
        
        # obtain length of time dimension
        time_length <-
          str_glue("{dir_raw_data}/{f}") %>%
          read_ncdf(proxy = T, make_time = F) %>%
          suppressMessages() %>%
          dim() %>%
          .[3]
        
        # sort across time >> slice last 10 days >> calculate the mean
        str_glue("cdo -seltimestep,{time_length-89}/{time_length} -timsort {dir_raw_data}/{f} {dir_cat}/mean_sel_{f}") %>%
          system(ignore.stdout = T, ignore.stderr = T)
        
      })
    
    # concatenate
    dir_cat %>%
      list.files(full.names = T) %>%
      str_subset("mean_sel") %>% 
      str_flatten(" ") %>%
      
      {system(str_glue("cdo cat {.} {dir_cat}/ninety_hottest_days.nc"),
              ignore.stdout = T, ignore.stderr = T)}
    
    # remove files
    dir_cat %>%
      list.files(full.names = T) %>%
      str_subset("mean_sel") %>% 
      walk(file.remove)
    
    # obtain threshold 
    "cdo timpctl,90 {dir_cat}/ninety_hottest_days.nc -timmin {dir_cat}/ninety_hottest_days.nc -timmax {dir_cat}/ninety_hottest_days.nc {dir_cat}/step_2.nc" %>% 
      str_glue() %>% 
      system(ignore.stdout = T, ignore.stderr = T)
    
    
    # 3-day running mean
    "cdo runmean,3 {dir_cat}/{v}_cat.nc {dir_cat}/step_0.nc" %>% 
      str_glue() %>% 
      system(ignore.stdout = T, ignore.stderr = T)
    # The time of outfile is determined by the time in the middle of all contributing timesteps of infile. 
    # This can be change with the CDO option --timestat_date <first|middle|last>.
    
    # # slice historical period 
    # "cdo selyear,1971/2000 {dir_cat}/step_0.nc {dir_cat}/step_1.nc" %>% 
    #   str_glue() %>% 
    #   system(ignore.stdout = T, ignore.stderr = T)
    # 
    # # obtain percentile
    # "cdo timpctl,90 {dir_cat}/step_1.nc -timmin {dir_cat}/step_1.nc -timmax {dir_cat}/step_1.nc {dir_cat}/step_2.nc" %>% 
    #   str_glue() %>% 
    #   system(ignore.stdout = T, ignore.stderr = T)
    
    
    # probability
    {
      
      # print(str_glue("      *PROBABILITY"))
      # 
      # derived_var <- tb_vars$var_derived[27]
      # 
      # outfile <-
      #   str_glue("{dir_derived}/{dom}_{derived_var}_yr_{rcm_}_{gcm_}_v2.nc")
      # 
      # "cdo yearmean -gec,0 -sub {dir_cat}/step_0.nc {dir_cat}/step_2.nc {outfile}" %>%
      #   str_glue() %>%
      #   system(ignore.stdout = T, ignore.stderr = T)
      
    }
    
    
    # duration
    {
     
      # print(str_glue(" ")) 
      # print(str_glue("      *DURATION"))
      # 
      # derived_var <- tb_vars$var_derived[28]
      # 
      # outfile <-
      #   str_glue("{dir_derived}/{dom}_{derived_var}_yr_{rcm_}_{gcm_}_v2.nc")
      # 
      # 
      # # identify days above threshold
      # str_glue("cdo gec,0 -sub {dir_cat}/step_0.nc {dir_cat}/step_2.nc {dir_cat}/step_3.nc") %>% 
      #   system(ignore.stdout = T, ignore.stderr = T)
      # 
      # load a proxy object to obtain dimensions
      # to tile and get time

      s_proxy <-
        str_glue("{dir_cat}/{v}_cat.nc") %>%
        read_ncdf(proxy = T) %>%
        suppressMessages()

      # obtain tiles

      chunks_index <- fn_tiling(s_proxy)
      lon_chunks <- chunks_index$lon_chunks
      lat_chunks <- chunks_index$lat_chunks

      # extract years

      all_years <-
        s_proxy %>%
        st_get_dimension_values("time") %>%
        year()

      # cut first and last
      # fits length of "step_0" map (3-day running mean)
      all_years_s <-
        all_years %>%
        tail(-1) %>%
        head(-1)

      unique_years <- all_years %>% unique()


      # # create temporary directory to save tiles
      # 
      # dir_chunks <- "/mnt/pers_disk/tmp"
      # dir.create(dir_chunks) 
      # 
      # 
      # iwalk(lon_chunks, function(lon_, i_lon){
      #   iwalk(lat_chunks, function(lat_, i_lat){
      #     
      #     print(str_glue("      processing chunk {i_lon} - {i_lat}"))
      #     
      #     # lon_ <- lon_chunks[[5]]
      #     # i_lon <- 5
      #     # 
      #     # lat_ <- lat_chunks[[3]]
      #     # i_lat <- 3
      #     
      #     
      #     # import data (3-day rolling mean)
      #     
      #     print(str_glue("      ---importing"))
      #     s <- 
      #       str_glue("{dir_cat}/step_3.nc") %>% 
      #       read_ncdf(proxy = T) %>% 
      #       suppressMessages() %>% 
      #       .[,lon_[1]:lon_[2], lat_[1]:lat_[2],] %>%
      #       st_as_stars(proxy = F)
      #       
      #     # calculate annual mean duration
      #     print(str_glue("      ---calculating"))
      #     s %>% 
      #       st_apply(c(1,2), function(x){
      #         
      #         if (any(is.na(x))) {
      #           
      #           rep(NA, length(unique_years))
      #           
      #         } else {
      #           
      #           map_dbl(unique_years, function(yr){
      #             
      #             r <- 
      #               x[all_years_s == yr] %>% 
      #               {c(0, .)} %>% # to make sure first period is 0
      #               rle() %>% 
      #               .$lengths
      #             
      #             if (length(r) > 1) {
      #               
      #               v <- mean(r[c(F,T)])
      #               ifelse(v > 365, 365, v)
      #               
      #             } else {
      #               
      #               0
      #               
      #             }
      #           })
      #           
      #         }  
      #         
      #       },
      #       .fname = "time",
      #       FUTURE = T) %>% 
      #       
      #       # st_as_stars(proxy = F) %>%
      #       
      #       aperm(c(2,3,1)) %>% 
      #       st_set_dimensions("time", values = unique_years) %>% 
      #       
      #       # save chunk
      #       write_stars(str_glue("{dir_chunks}/{dom}_tmpfile_{i_lon}_{i_lat}.tif"))
      #     
      #     gc()
      #     
      #   })
      # })
      # 
      # 
      # # mosaic chunks row-wise
      # rows_ <- 
      #   map(seq_along(lat_chunks), function(i_lat){
      #     
      #     # build a table to sort tiles
      #     # and ensure they are imported in order
      #     tibble(
      #       file = dir_chunks %>% 
      #         list.files(full.names = T) %>% 
      #         str_subset(str_glue("_{i_lat}.tif"))) %>% 
      #       mutate(col = str_extract(file, "_[:digit:]*_"),
      #              col = parse_number(col)) %>% 
      #       arrange(col) %>% 
      #       pull(file) %>%
      #       
      #       # import
      #       read_stars(along = 1)
      #   })
      # 
      # # mosaic rows
      # mos <-
      #   rows_ %>%
      #   {do.call(c, c(., along = 2))} %>%
      #   st_set_dimensions(1, names = "lon", values = st_get_dimension_values(s_proxy, "lon")) %>%
      #   st_set_dimensions(2, names = "lat", values = st_get_dimension_values(s_proxy, "lat")) %>%
      #   st_set_crs(4326) %>%
      #   st_set_dimensions(3, 
      #                     names = "time", 
      #                     # values = str_glue("{all_years}0101") %>% 
      #                     values = str_glue("{unique_years}0101") %>% 
      #                       as_date() %>% 
      #                       as.numeric()) %>%
      #   setNames("days")
      # 
      # 
      # fn_write_nc(mos, outfile, "time", "days since 1970-01-01", un = "")
      # 
      # unlink(dir_chunks, recursive = T)
      
    }
        
     
      
    # intensity
    {
      
      print(str_glue(" "))
      print(str_glue("      *INTENSITY"))
      
      derived_var <- tb_vars$var_derived[29]
      
      # print(str_glue())
      
      outfile <-
        str_glue("{dir_derived}/{dom}_{derived_var}_yr_{rcm_}_{gcm_}_v2.nc")
      
      # # load a proxy object to obtain dimensions
      # # to tile and get time 
      # 
      # s_proxy <- 
      #   str_glue("{dir_cat}/{v}_cat.nc") %>% 
      #   read_ncdf(proxy = T) %>% 
      #   suppressMessages()
      # 
      # # obtain tiles
      # 
      # chunks_index <- fn_tiling(s_proxy)
      # lon_chunks <- chunks_index$lon_chunks
      # lat_chunks <- chunks_index$lat_chunks
      # 
      # # extract years
      # 
      # all_years <- 
      #   s_proxy %>% 
      #   st_get_dimension_values("time") %>% 
      #   year()
      # 
      # # cut first and last
      # # fits length of "step_0" map (3-day running mean)
      # all_years_s <- 
      #   all_years %>% 
      #   tail(-1) %>% 
      #   head(-1)
      # 
      # unique_years <- all_years %>% unique()
      
      step_2 <- 
        "/mnt/pers_disk/cat/step_2.nc" %>% 
        read_ncdf(make_time = F, proxy = F) %>% 
        suppressMessages()
      
      
      
      # create temporary directory to save tiles
      
      dir_chunks <- "/mnt/pers_disk/tmp"
      dir.create(dir_chunks) 
      
      iwalk(lon_chunks, function(lon_, i_lon){
        iwalk(lat_chunks, function(lat_, i_lat){

          print(str_glue("      processing chunk {i_lon} - {i_lat}"))

          # lon_ <- lon_chunks[[1]]
          # i_lon <- 12
          # 
          # lat_ <- lat_chunks[[2]]
          # i_lat <- 3


          # import data (3-day rolling mean)
          print(str_glue("      ---importing"))
          # run_mean <-
          #   str_glue("{dir_cat}/step_0.nc") %>%
          #   read_ncdf(proxy = T) %>%
          #   suppressMessages() %>%
          #   .[,lon_[1]:lon_[2], lat_[1]:lat_[2],] %>%
          #   st_as_stars(proxy = F)
          
          run_mean <-
            str_glue("{dir_cat}/step_0.nc") %>%
            read_ncdf(proxy = F,
                      ncsub = cbind(
                        start = c(lon_[1], lat_[1], 1),
                        count = c(lon_[2] - lon_[1] + 1,
                                  lat_[2] - lat_[1] + 1,
                                  NA)
                      )) %>%
            suppressMessages()

          # threshold <-
          #   str_glue("{dir_cat}/step_2.nc") %>%
          #   read_ncdf(proxy = T, make_time = F) %>%
          #   suppressMessages() %>%
          #   .[,lon_[1]:lon_[2], lat_[1]:lat_[2],] %>%
          #   st_as_stars(proxy = F)

          threshold <-
            step_2 %>% st_warp(run_mean)
          
          # threshold <- 
          #   step_2[,lon_[1]:lon_[2], lat_[1]:lat_[2],]

          print(str_glue("      ---calculating"))
          c(threshold, run_mean, along = "time") %>%

            st_apply(c(1,2), function(x){

              th <- x[1]
              x_ <- tail(x, -1)
              
              if (any(is.na(x_))) {
                
                r <- rep(NA, length(unique_years))
                
              } else {
                
                r <- 
                  
                  map_dbl(unique_years, function(yr){
                    
                    x_[all_years_s == yr] %>%
                      .[. > th] %>%
                      mean() %>%
                      # {. - th} %>%
                      {ifelse(is.nan(.), NA, .)}
                    
                  })
                
              }
              
              gc()
              return(r)


            },
            .fname = "time",
            FUTURE = T) %>%

            aperm(c(2,3,1)) %>%
            st_set_dimensions("time", values = unique_years) %>%

            # save chunk
            write_stars(str_glue("{dir_chunks}/{dom}_tmpfile_{i_lon}_{i_lat}.tif"))
          
          gc()

        })
      })
      
      
      # mosaic chunks row-wise
      rows_ <- 
        map(seq_along(lat_chunks), function(i_lat){
          
          # build a table to sort tiles
          # and ensure they are imported in order
          tibble(
            file = dir_chunks %>% 
              list.files(full.names = T) %>% 
              str_subset(str_glue("_{i_lat}.tif"))) %>% 
            mutate(col = str_extract(file, "_[:digit:]*_"),
                   col = parse_number(col)) %>% 
            arrange(col) %>% 
            pull(file) %>%
            
            # import
            read_stars(along = 1)
        })
      
      # mosaic rows
      mos <-
        rows_ %>%
        {do.call(c, c(., along = 2))} %>%
        st_set_dimensions(1, names = "lon", values = st_get_dimension_values(s_proxy, "lon")) %>%
        st_set_dimensions(2, names = "lat", values = st_get_dimension_values(s_proxy, "lat")) %>%
        st_set_crs(4326) %>%
        st_set_dimensions(3, 
                          names = "time", 
                          # values = str_glue("{all_years}0101") %>% 
                          values = str_glue("{unique_years}0101") %>% 
                            as_date() %>% 
                            as.numeric()) %>%
        setNames("days")
      
      
      fn_write_nc(mos, outfile, "time", "days since 1970-01-01", un = "K")
      
      unlink(dir_chunks, recursive = T)
      
    }
    
        
      
    
    
    
    # delete concatenated file
    unlink(dir_cat, recursive = T)
    
    # delete raw files
    unlink(dir_raw_data, recursive = T)
    
  # }
  
}






