
fn_derived <- function(derived_var){
  
  outfile <-
    str_glue("{dir_derived}/{dom}_{derived_var}_yr_{rcm_}_{gcm_}.nc")
  
  if(file.exists(outfile)){
    file.remove(outfile)
    print(str_glue("      (old derived file removed)"))
  }
  
  
  # HEAT VOLUME ---------------------------------------------------------------
  
  
  ## MAX TEMPERATURE ------------------------------
  
  
  if(derived_var == "days-gte-32C-tasmax"){
    
    set_units(32, degC) %>%
      set_units(K) %>%
      drop_units() -> lim_k
    
    str_glue("cdo -yearsum -gec,{lim_k} {dir_cat}/{v}_cat.nc {outfile}") %>%
      system(ignore.stdout = T, ignore.stderr = T)
    
    
    
    # *************
    
    
  } else if(derived_var == "days-gte-35C-tasmax"){
    
    set_units(35, degC) %>%
      set_units(K) %>%
      drop_units() -> lim_k
    
    str_glue("cdo -yearsum -gec,{lim_k} {dir_cat}/{v}_cat.nc {outfile}") %>%
      system(ignore.stdout = T, ignore.stderr = T)
    
    
    
    # *************
    
    
  } else if(derived_var == "days-gte-38C-tasmax"){
    
    set_units(38, degC) %>%
      set_units(K) %>%
      drop_units() -> lim_k
    
    str_glue("cdo -yearsum -gec,{lim_k} {dir_cat}/{v}_cat.nc {outfile}") %>%
      system(ignore.stdout = T, ignore.stderr = T)
    
    
    
    # *************
    
    
  } else if(derived_var == "ten-hottest-days-tasmax"){
    
    dir_temp <- str_glue("{dir_tmp}/dir_temp")
    dir.create(dir_temp)
    
    # loop through annual files
    dir_raw_data %>%
      list.files() %>%
      future_walk(function(f){
        
        # obtain length of time dimension
        time_length <-
          str_glue("{dir_raw_data}/{f}") %>%
          read_ncdf(proxy = T, make_time = F) %>%
          suppressMessages() %>%
          dim() %>%
          .[3]
        
        # sort across time >> slice last 10 days >> calculate the mean
        str_glue("cdo -timmean -seltimestep,{time_length-9}/{time_length} -timsort {dir_raw_data}/{f} {dir_temp}/mean_sel_{f}") %>%
          system(ignore.stdout = T, ignore.stderr = T)
        
      })
    
    # concatenate and save
    dir_temp %>%
      list.files(full.names = T) %>%
      str_flatten(" ") %>%
      
      {system(str_glue("cdo cat {.} {outfile}"),
              ignore.stdout = T, ignore.stderr = T)}
    
    # delete intermediate files
    unlink(dir_temp, recursive = T)
    
    
    
    # *************
    
    
  } else if(derived_var == "mean-tasmax"){
    
    str_glue("cdo yearmean {dir_cat}/{v}_cat.nc {outfile}") %>%
      system(ignore.stdout = T, ignore.stderr = T)
    
    
    
    # *************
    
    
  } else if(derived_var == "days-lt-0C-tasmax"){
    
    set_units(0, degC) %>%
      set_units(K) %>%
      drop_units() -> lim_k
    
    str_glue("cdo -yearsum -ltc,{lim_k} {dir_cat}/{v}_cat.nc {outfile}") %>%
      system(ignore.stdout = T, ignore.stderr = T)
    
    
    
    
    ## AVERAGE TEMPERATURE --------------------------
    
    
  } else if(derived_var == "mean-tasmean"){
    
    str_glue("cdo yearmean {dir_cat}/{v}_cat.nc {outfile}") %>%
      system(ignore.stdout = T, ignore.stderr = T)
    
    
    
    ## MIN TEMPERATURE ------------------------------ 
    
    
  } else if(derived_var == "days-gte-20C-tasmin"){
    
    set_units(20, degC) %>%
      set_units(K) %>%
      drop_units() -> lim_k
    
    str_glue("cdo -yearsum -gec,{lim_k} {dir_cat}/{v}_cat.nc {outfile}") %>%
      system(ignore.stdout = T, ignore.stderr = T)
    
    
    
    # *************
    
    
  } else if(derived_var == "days-gte-25C-tasmin"){
    
    set_units(25, degC) %>%
      set_units(K) %>%
      drop_units() -> lim_k
    
    str_glue("cdo -yearsum -gec,{lim_k} {dir_cat}/{v}_cat.nc {outfile}") %>%
      system(ignore.stdout = T, ignore.stderr = T)
    
    
    
    # *************
    
    
  } else if(derived_var == "mean-tasmin"){
    
    str_glue("cdo yearmean {dir_cat}/{v}_cat.nc {outfile}") %>%
      system(ignore.stdout = T, ignore.stderr = T)
    
    
    
    # *************
    
    
  } else if(derived_var == "days-lt-0C-tasmin"){
    
    set_units(0, degC) %>%
      set_units(K) %>%
      drop_units() -> lim_k
    
    str_glue("cdo -yearsum -ltc,{lim_k} {dir_cat}/{v}_cat.nc {outfile}") %>%
      system(ignore.stdout = T, ignore.stderr = T)
    
    
    
    ## WETBULB TEMPERATURE --------------------------
    
    
  } else if(derived_var == "days-gte-26C-wetbulb"){
    
    lim_k <- 26
    
    str_glue("cdo -yearsum -gec,{lim_k} {dir_cat}/{v}_cat.nc {outfile}") %>%
      system(ignore.stdout = T, ignore.stderr = T)
    
    
    
    # *************
    
    
  } else if(derived_var == "days-gte-28C-wetbulb"){
    
    lim_k <- 28
    
    str_glue("cdo -yearsum -gec,{lim_k} {dir_cat}/{v}_cat.nc {outfile}") %>%
      system(ignore.stdout = T, ignore.stderr = T)
    
    
    
    # *************
    
    
  } else if(derived_var == "days-gte-30C-wetbulb"){
    
    lim_k <- 30
    
    str_glue("cdo -yearsum -gec,{lim_k} {dir_cat}/{v}_cat.nc {outfile}") %>%
      system(ignore.stdout = T, ignore.stderr = T)
    
    
    
    # *************
    
    
  } else if(derived_var == "days-gte-32C-wetbulb"){
    
    lim_k <- 32
    
    str_glue("cdo -yearsum -gec,{lim_k} {dir_cat}/{v}_cat.nc {outfile}") %>%
      system(ignore.stdout = T, ignore.stderr = T)
    
    
    
    # *************
    
    
  } else if(derived_var == "ten-hottest-days-wetbulb"){
    
    dir_temp <- str_glue("{dir_tmp}/dir_temp")
    dir.create(dir_temp)
    
    # loop through annual files
    dir_raw_data %>%
      list.files() %>%
      future_walk(function(f){
        
        # obtain length of time dimension
        time_length <-
          str_glue("{dir_raw_data}/{f}") %>%
          read_ncdf(proxy = T, make_time = F) %>%
          suppressMessages() %>%
          dim() %>%
          .[3]
        
        # sort across time >> slice last 10 days >> calculate the mean
        str_glue("cdo -timmean -seltimestep,{time_length-9}/{time_length} -timsort {dir_raw_data}/{f} {dir_temp}/mean_sel_{f}") %>%
          system(ignore.stdout = T, ignore.stderr = T)
        
      })
    
    # concatenate and save
    dir_temp %>%
      list.files(full.names = T) %>%
      str_flatten(" ") %>%
      
      {system(str_glue("cdo cat {.} {outfile}"),
              ignore.stdout = T, ignore.stderr = T)}
    
    # delete intermediate files
    unlink(dir_temp, recursive = T)
    
    
    
    
    # WATER VOLUME ----------------------------------------------------------------
    
    
    ## PRECIPITATION --------------------------------
    
    
  } else if(derived_var == "total-precip"){
    
    str_glue("cdo yearsum {dir_cat}/{v}_cat.nc {outfile}") %>%
      system(ignore.stdout = T, ignore.stderr = T)
    
    
    
    # *************
    
    
  } else if(derived_var == "ninety-wettest-days"){
    
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
    
    time_dim <- 
      s_proxy %>% 
      st_get_dimension_values("time") %>% 
      year()
    
    all_years <- time_dim %>% unique()
    
    
    # create temporary directory to save tiles
    
    dir_tmp <- "/mnt/pers_disk/tmp"
    dir.create(dir_tmp)
    
    
    # loop through chunks
    # calculate precip of 90 wettest days for each
    
    iwalk(lon_chunks, function(lon_, i_lon){
      iwalk(lat_chunks, function(lat_, i_lat){
        
        print(str_glue("      processing chunk {i_lon} - {i_lat}"))
        
        s_proxy[,lon_[1]:lon_[2], lat_[1]:lat_[2],] %>% 
          
          st_apply(
            
            c(1,2),
            
            # function to identify annual maximas
            # while preventing their overlap:
            
            function(x){
              
              if(any(is.na(x))){
                
                pr <- rep(NA, length(all_years))
                
              } else {
                
                # running sum of 90 days
                # value assigned to last obs of the window
                runsum <- 
                  x %>% 
                  slider::slide_dbl(sum, 
                                    .before = 89, 
                                    .complete = T, 
                                    .step = 2)
                
                # initialize vector
                pr <- rep(NA_real_, length(all_years))
                
                dy <- 0 # first iteration; no day
                
                # loop through years
                for(i in 1:length(all_years)){
                  
                  time_range <- which(time_dim == all_years[i]) %>% range()
                  
                  # if max is within the last 90 days of the year
                  # shorten the range of time to look for max
                  # so that windows do not overlap
                  if(i != 1 & dy >= time_range[1]-90){
                    
                    dy <- 
                      which.max(runsum[(dy+90):time_range[2]])+(dy+90)-1
                    
                  } else {
                    
                    dy <- 
                      which.max(runsum[time_range[1]:time_range[2]])+time_range[1]-1
                    
                  }
                  
                  pr[i] <- runsum[dy]
                  
                }
              }
              
              return(pr)
              
            }, # end of function
            
            FUTURE = T,
            .fname = "time") %>% 
          
          st_as_stars(proxy = F) %>%
          aperm(c(2,3,1)) %>% 
          
          # save chunk
          write_stars(str_glue("{dir_tmp}/{dom}_tmpfile_{i_lon}_{i_lat}.tif"))
        
      })
    })
    
    
    # mosaic chunks row-wise
    rows_ <- 
      map(seq_along(lat_chunks), function(i_lat){
        
        # build a table to sort tiles
        # and ensure they are imported in order
        tibble(
          file = dir_tmp %>% 
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
                        values = str_glue("{all_years}0101") %>% 
                          as_date() %>% 
                          as.numeric()) %>%
      setNames("pr")
    
    
    fn_write_nc(mos, outfile, "time", "days since 1970-01-01", un = "kg/m^2/s")
    
    unlink(dir_tmp, recursive = T)
    
    
    
    ## PRECIP + AVG. TEMPERATURE ------------------
    
    
  } else if(derived_var == "days-gte-1mm-precip-lt-0C-tasmean"){
    
    c("pr", "tas") %>% 
      future_walk(function(v){
        
        if(v == "pr"){
          
          set_units(1, kg/m^2/d) %>%
            set_units(kg/m^2/s) %>%
            drop_units() -> lim_v
          
          str_glue("cdo gec,{lim_v} {dir_cat}/pr_cat.nc {dir_cat}/pr_step1.nc") %>% 
            system(ignore.stdout = T, ignore.stderr = T)
          
          
        } else {
          
          set_units(0, degC) %>%
            set_units(K) %>%
            drop_units() -> lim_v
          
          str_glue("cdo ltc,{lim_v} {dir_cat}/tas_cat.nc {dir_cat}/tas_step1.nc") %>% 
            system(ignore.stdout = T, ignore.stderr = T)
          
        }
        
      })
    
    
    # joint condition
    str_glue("cdo -yearsum -gec,2 -add {dir_cat}/pr_step1.nc {dir_cat}/tas_step1.nc {outfile}") %>% 
      system(ignore.stdout = T, ignore.stderr = T)
    
    
    
    ## PRECIP + MAX. TEMPERATURE ------------------
    
    
  } else if(derived_var == "days-gte-b90perc-tasmax-lt-b10perc-precip"){
    
    future_walk(c("pr", "tasmax"), function(v){
      
      # params
      if(v == "tasmax"){
        thresh <- 90
        command <- "gec"
        
      } else if(v == "pr") {
        thresh <- 10
        command <- "ltc"
      }
      
      # subset baseline
      str_glue("cdo selyear,1971/2000 {dir_cat}/{v}_cat.nc {dir_cat}/{v}_step1.nc") %>% 
        system(ignore.stdout = T, ignore.stderr = T)
      
      # calculate percentile
      str_glue("cdo timpctl,{thresh} {dir_cat}/{v}_step1.nc -timmin {dir_cat}/{v}_step1.nc -timmax {dir_cat}/{v}_step1.nc {dir_cat}/{v}_step2.nc") %>% 
        system(ignore.stdout = T, ignore.stderr = T)
      
      # obtain no. days under/above baseline percentile
      str_glue("cdo -{command},0 -sub {dir_cat}/{v}_cat.nc {dir_cat}/{v}_step2.nc {dir_cat}/{v}_step3.nc") %>% 
        system(ignore.stdout = T, ignore.stderr = T)
      
    })
    
    # joint condition
    
    ff <- 
      dir_cat %>% 
      list.files(full.names = T) %>% 
      str_subset("_cat", negate = T) %>% 
      str_subset("step3") %>% 
      str_flatten(" ")
    
    str_glue("cdo -yearsum -gec,2 -add {ff} {outfile}") %>% 
      system(ignore.stdout = T, ignore.stderr = T)
    
    
    
    
    
    # LAND VOLUME --------------------------------------------------------------
    
    
    
    # SPEI --------------------------------------
    
    
  } else if(derived_var == "mean-spei"){
    
    str_glue("cdo yearmean {dir_cat}/{v}_cat.nc {outfile}") %>%
      system(ignore.stdout = T, ignore.stderr = T)
    
    
    # *************
    
    
    
  } else if(derived_var == "prop-months-lte-neg0.8-spei"){
    
    str_glue("cdo yearmean -lec,-0.8 {dir_cat}/{v}_cat.nc {outfile}") %>%
      system(ignore.stdout = T, ignore.stderr = T)
    
    
    # *************
    
    
    
  } else if(derived_var == "prop-months-lte-neg1.6-spei"){
    
    str_glue("cdo yearmean -lec,-1.6 {dir_cat}/{v}_cat.nc {outfile}") %>%
      system(ignore.stdout = T, ignore.stderr = T)
    
    
    
    ## FWI --------------------------------------
    
    
    
  } else if(derived_var == "days-gte-b95perc-fwi"){
    
    # subset baseline
    str_glue("cdo selyear,1972/2000 {dir_cat}/{v}_cat.nc {dir_cat}/{v}_step1.nc") %>% 
      system(ignore.stdout = T, ignore.stderr = T)
    
    # calculate percentile
    str_glue("cdo timpctl,95 {dir_cat}/{v}_step1.nc -timmin {dir_cat}/{v}_step1.nc -timmax {dir_cat}/{v}_step1.nc {dir_cat}/{v}_step2.nc") %>% 
      system(ignore.stdout = T, ignore.stderr = T)
    
    # obtain no. days above baseline percentile; then sum per year
    str_glue("cdo -yearsum -gec,0 -sub {dir_cat}/{v}_cat.nc {dir_cat}/{v}_step2.nc {outfile}") %>% 
      system(ignore.stdout = T, ignore.stderr = T)
    
    
    
  }
  
  
  
  # verify correct time dimension
  time_steps <-
    outfile %>%
    read_ncdf(proxy = T, make_time = F) %>%
    suppressMessages() %>%
    suppressWarnings() %>%
    st_get_dimension_values("time") %>%
    length()
  
  print(str_glue("      Done: new file with {time_steps} timesteps ({derived_var})"))
  
}

