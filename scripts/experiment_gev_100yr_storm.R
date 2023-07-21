
library(tidyverse)
library(lubridate)
library(stars)
library(furrr)
library(units)

options(future.fork.enable = T)
plan(multicore)


# # load main function to calculate derived vars
# source("scripts/fn_derived.R") 
source("scripts/functions.R") # other functions


dir_cordex <- "/mnt/bucket_cmip5/RCM_regridded_data"
dir_derived <- "/mnt/bucket_mine/results/global_heat_pf/01_derived"
dir_tmp <- "/mnt/pers_disk"

# doms <- c("SEA", "AUS", "CAS", "WAS", "EAS", "AFR", "EUR", "NAM", "CAM", "SAM")

# load table of variables
# tb_vars <-
#   read_csv("/mnt/bucket_mine/pf_variable_table.csv") %>% 
#   suppressMessages()


dom <- "AFR"



# assemble table of needed files for calculation
tb_files <-
  fn_data_table("precipitation") %>% 
  filter(str_detect(file, "MISSING", negate = T))

# extract models
tb_models <-
  unique(tb_files[, c("gcm", "rcm")]) %>% 
  arrange(rcm, gcm)

# # ignore RegCM in these domains  
# if(dom %in% c("SAM", "AUS", "CAS")){
#   
#   tb_models <- 
#     tb_models %>% 
#     filter(str_detect(rcm, "RegCM", negate = T))
#   
# }
#   
  

# MODEL LOOP ----------------------------------------------------------------

for(i in seq_len(nrow(tb_models))){
  
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
  
  
  
  # BLOCK MAX ------------------------------------------------------------------
  
  outfile <-
    str_glue("/mnt/pers_disk/block_max/{dom}_1day-max-precip_yr_{rcm_}_{gcm_}.nc")
  
  if(file.exists(outfile)){
    file.remove(outfile)
    print(str_glue("      (old derived file removed)"))
  }
  
  str_glue("cdo yearmax {dir_cat}/{v}_cat.nc {outfile}") %>%
    system(ignore.stdout = T, ignore.stderr = T)

  # verify correct time dimension
  time_steps <-
    outfile %>%
    read_ncdf(proxy = T, make_time = F) %>%
    suppressMessages() %>%
    suppressWarnings() %>%
    st_get_dimension_values("time") %>%
    length()
  
  print(str_glue("      Done: new file with {time_steps} timesteps"))
  
  # delete concatenated file
  unlink(dir_cat, recursive = T)
  
  # delete raw files
  unlink(dir_raw_data, recursive = T)

}  





# TESTS ENSEMBLE FIRST ---------------------------------------------------------



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



ff <- 
  "/mnt/pers_disk/block_max/" %>% 
  list.files() %>% 
  str_subset(dom)

l_s <- 
  ff %>% 
  future_map(function(f){
    
    read_ncdf(str_glue("/mnt/pers_disk/block_max/{f}"), 
              proxy = F) %>% 
      suppressMessages() %>% 
      suppressWarnings() %>% 
      setNames("v") %>% 
      mutate(v = set_units(v, kg/m^2/d))
  }) %>% 
  
  map(function(s){
    
    s %>%
      st_set_dimensions("time",
                        values = st_get_dimension_values(s, "time") %>%
                          as.character() %>%
                          str_sub(end = 4) %>% 
                          as.integer()) %>%
      
      mutate(v = set_units(v, NULL))
  })




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




# PROCESS DATA


windw <- 1

baseline <- l_s_wl[[1]] %>% pull() 

sub_vect_lon <- seq_len(dim(baseline)[1]) %>% tail(-windw) %>% head(-windw)
sub_vect_lat <- seq_len(dim(baseline)[2]) %>% tail(-windw) %>% head(-windw)


baseline_stats <- 
  
  future_map_dfr(sub_vect_lon, function(i_lon){
    map_df(sub_vect_lat, function(i_lat){
      
      p <- 
        baseline[(i_lon-windw):(i_lon+windw), (i_lat-windw):(i_lat+windw), ] %>%
        as.vector()
      
      if(any(is.na(p))){
        
        lev <- NA
        prob <- NA
        
      } else {
        
        params <- lmom::pelgev(lmom::samlmu(p))
        
        lev <- lmom::quagev(0.99,
                            para = params)
        prob <- 0.01
      }
      
      res <- c(lev = lev,
               prob = prob)
      
      return(res)
      
    })
  })


baseline_stats <- 
  map(c("lev", "prob"), function(colmn){
    
    s <- 
      baseline_stats %>%
      pull(colmn) %>% 
      matrix(nrow = length(sub_vect_lon), ncol = length(sub_vect_lat), byrow = T) %>% 
      {cbind(NA, .)} %>% 
      {cbind(., NA)} %>% 
      {rbind(NA, .)} %>% 
      {rbind(., NA)} %>% 
      st_as_stars() %>% 
      setNames(colmn)
    
    st_dimensions(s) <- st_dimensions(l_s_wl[[1]] %>% slice(time, 1))
    
    return(s)
    
  }) %>% 
  do.call(c, .)





baseline_lev <- 
  baseline_stats %>%
  select(1) %>% 
  pull()


wl_stats <- 
  
  l_s_wl[2:6] %>% 
  map(function(swl){
    
    # a <- abind(baseline_lev, pull(swl), along = 3)
    a <- swl %>% pull()
    
    wl_stats <- 
      future_map_dfr(sub_vect_lon, function(i_lon){
        map_df(sub_vect_lat, function(i_lat){
          
          b <- baseline_lev[i_lon, i_lat]
          
          p <- 
            a[(i_lon-windw):(i_lon+windw), (i_lat-windw):(i_lat+windw), ] %>% 
            as.vector()
          
          
          if(any(is.na(p))){
            
            lev <- NA
            prob <- NA
            
          } else {
            
            params <- lmom::pelgev(lmom::samlmu(p))
            
            lev <- lmom::quagev(0.99,
                                para = params)
            
            prob <- 1 - lmom::cdfgev(b,
                                     para = params)
            
          }
          
          res <- c(lev = lev,
                   prob = prob)
          
          return(res)
          
        })
      }) 
    
    
    wl_stats <- 
      map(c("lev", "prob"), function(colmn){
        
        s <- 
          wl_stats %>%
          pull(colmn) %>% 
          matrix(nrow = length(sub_vect_lon), ncol = length(sub_vect_lat), byrow = T) %>% 
          {cbind(NA, .)} %>% 
          {cbind(., NA)} %>% 
          {rbind(NA, .)} %>% 
          {rbind(., NA)} %>% 
          st_as_stars() %>% 
          setNames(colmn)
        
        st_dimensions(s) <- st_dimensions(swl %>% slice(time, 1))
        
        return(s)
        
      }) %>% 
      do.call(c, .)
    
    
    
  })



hundred_yr_storm <- 
  c(list(baseline_stats), wl_stats)%>% 
  {do.call(c, c(., along = "wl"))} %>% 
  st_set_dimensions(3, values = seq(0.5, 3, 0.5))




# PLOT 

pf_pal <- c("#ffab24", "#515866", "#25a8b7", "#007ea7", "#003459")

land_pol <- 
  "/mnt/bucket_mine/misc_data/ne_110m_land/" %>% 
  st_read() %>%
  mutate(a = 1) %>% 
  select(a)

land_rast_0.1 <- 
  c(st_point(c(-180, -90)),
    st_point(c(180, 90))) %>%
  st_bbox() %>%
  st_set_crs(4326) %>% 
  st_as_stars(dx = 0.1, dy = 0.1, values = NA) %>% 
  {st_rasterize(land_pol, .)}

land_rast_0.2 <- 
  st_warp(land_rast_0.1, hundred_yr_storm %>% slice(wl, 1))


hundred_yr_storm[is.na(land_rast_0.2)] <- NA


# AMOUNTS *******************************

# RAW VALUES
hundred_yr_storm %>% 
  select(lev) %>% 
  as_tibble() %>% 
  mutate(lev = raster::clamp(lev, 100, 700)) %>%
  
  ggplot(aes(lon,lat, fill = lev)) +
  geom_raster() +
  facet_wrap(~wl, 3, 3) +
  colorspace::scale_fill_binned_sequential("plasma",
                                           rev = F,
                                           n.breaks = 7,
                                           na.value = "transparent") +
  coord_equal()


# CHANGE
hundred_yr_storm %>% 
  select(lev) %>% 
  split("wl") %>%
  setNames(wls) %>% 
  as_tibble() %>%
  mutate(across(`1.0`:`3.0`, ~.x - `0.5`)) %>% 
  pivot_longer(`0.5`:`3.0`, names_to = "wl", values_to = "lev") %>% 
  mutate(lev = ifelse(wl == "0.5", NA, lev),
         lev = round(lev),
         lev = case_when(lev < -1 ~ "< -1",
                         lev >= -1 & lev <= 11 ~ "-1-+11",
                         lev >= 12 & lev <= 24 ~ "+12-+24",
                         lev >= 25 & lev <= 50 ~ "+25-+50",
                         lev > 50 ~ "> +50") %>% 
           factor(levels = c("< -1", "-1-+11", "+12-+24", "+25-+50", "> +50"))) %>% 
  
  
  ggplot(aes(lon,lat, fill = lev)) +
  geom_raster() +
  facet_wrap(~wl, 3, 3) +
  # colorspace::scale_fill_binned_divergingx("RdYlBu",
  #                                          rev = F,
  #                                          breaks = c(-1, 10, 25, 50),
  #                                          na.value = "transparent",
  #                                          mid = 2) +
  scale_fill_manual(values = pf_pal, na.value = "transparent", na.translate = F) +
  coord_equal() +
  theme(axis.title = element_blank()) +
  labs(title = "Change in Precipitation of 100 yr storm")



# FREQS ********************************

# RAW VALUES
hundred_yr_storm %>% 
  select(prob) %>% 
  as_tibble() %>% 
  mutate(prob = ifelse(wl == 0.5, NA, prob),
         prob = raster::clamp(prob, 0.01, 0.06)) %>%
  
  ggplot(aes(lon,lat, fill = prob)) +
  geom_raster() +
  facet_wrap(~wl, 3, 3) +
  colorspace::scale_fill_binned_sequential("plasma",
                                           rev = F,
                                           n.breaks = 7,
                                           na.value = "transparent") +
  coord_equal()

# CHANGE
hundred_yr_storm %>% 
  select(prob) %>% 
  split("wl") %>%
  setNames(wls) %>% 
  as_tibble() %>%
  mutate(across(`1.0`:`3.0`, ~.x/0.01)) %>% 
  pivot_longer(`0.5`:`3.0`, names_to = "wl", values_to = "prob") %>% 
  mutate(prob = ifelse(wl == "0.5", NA, prob),
         prob = round(prob),
         prob = case_when(prob < 1 ~ "< 1",
                          prob == 1 ~ "1",
                          prob == 2 ~ "2",
                          prob == 3 | prob == 4 ~ "3-4",
                          prob > 4 ~ "> 4") %>% 
           factor(levels = c("< 1", "1", "2", "3-4", "> 4"))) %>%  
  
  
  ggplot(aes(lon,lat, fill = prob)) +
  geom_raster() +
  facet_wrap(~wl, 3, 3) +
  # colorspace::scale_fill_binned_divergingx("RdYlBu",
  #                                          rev = F,
  #                                          breaks = c(0, 1, 2, 3, 4, 5),
  #                                          na.value = "transparent",
  #                                          mid = 1) +
  scale_fill_manual(values = pf_pal, na.value = "transparent", na.translate = F) +
  coord_equal() + 
  theme(axis.title = element_blank()) +
  labs(title = "Change in Frequency of 100 yr storm (X times +/- frequent)")











  

# TEST ENSEMBLE LATER ---------------------------------------------------------




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



ff <- 
  "/mnt/pers_disk/block_max/" %>% 
  list.files() %>% 
  str_subset(dom)

l_s <- 
  ff %>% 
  future_map(function(f){
    
    read_ncdf(str_glue("/mnt/pers_disk/block_max/{f}"), 
              proxy = F) %>% 
      suppressMessages() %>% 
      suppressWarnings() %>% 
      setNames("v") %>% 
      mutate(v = set_units(v, kg/m^2/d))
  }) %>% 
  
  map(function(s){
    
    s %>%
      st_set_dimensions("time",
                        values = st_get_dimension_values(s, "time") %>%
                          as.character() %>%
                          str_sub(end = 4) %>% 
                          as.integer()) %>%
      
      mutate(v = set_units(v, NULL))
  })



l_s_wl <- 
  
  # loop through warming levels
  map(wls %>% set_names(), function(wl){
    
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
      
    }) #%>% 
      
      # concatenate all models and form a single time dimension
      #{do.call(c, c(., along = "time"))}
    
  })






# PROCESS ****************************

l_s_mod_wl <- transpose(l_s_wl)

windw <- 1

hundred_yr_storm <- 
  map(1:6, function(mod){
    
    l_s_wl <- l_s_mod_wl %>% pluck(mod)
    
    
    baseline <- l_s_wl[[1]] %>% pull() 
    
    sub_vect_lon <- seq_len(dim(baseline)[1]) %>% tail(-windw) %>% head(-windw)
    sub_vect_lat <- seq_len(dim(baseline)[2]) %>% tail(-windw) %>% head(-windw)
    
    
    baseline_stats <- 
      
      future_map_dfr(sub_vect_lon, function(i_lon){
        map_df(sub_vect_lat, function(i_lat){
          
          p <- 
            baseline[(i_lon-windw):(i_lon+windw), (i_lat-windw):(i_lat+windw), ] %>%
            as.vector()
          
          if(any(is.na(p))){
            
            lev <- NA
            prob <- NA
            
          } else {
            
            params <- lmom::pelgev(lmom::samlmu(p))
            
            lev <- lmom::quagev(0.99,
                                para = params)
            prob <- 0.01
          }
          
          res <- c(lev = lev,
                   prob = prob)
          
          return(res)
          
        })
      })
    
    baseline_stats <- 
      map(c("lev", "prob"), function(colmn){
        
        s <- 
          baseline_stats %>%
          pull(colmn) %>% 
          matrix(nrow = length(sub_vect_lon), ncol = length(sub_vect_lat), byrow = T) %>% 
          {cbind(NA, .)} %>% 
          {cbind(., NA)} %>% 
          {rbind(NA, .)} %>% 
          {rbind(., NA)} %>% 
          st_as_stars() %>% 
          setNames(colmn)
        
        st_dimensions(s) <- st_dimensions(l_s_wl[[1]] %>% slice(time, 1))
        
        return(s)
        
      }) %>% 
      do.call(c, .)
    
    
    
    
    
    baseline_lev <- 
      baseline_stats %>%
      select(1) %>% 
      pull()
    
    
    wl_stats <- 
      
      l_s_wl[2:6] %>% 
      map(function(swl){
        
        # a <- abind(baseline_lev, pull(swl), along = 3)
        a <- swl %>% pull()
        
        wl_stats <- 
          future_map_dfr(sub_vect_lon, function(i_lon){
            map_df(sub_vect_lat, function(i_lat){
              
              b <- baseline_lev[i_lon, i_lat]
              
              p <- 
                a[(i_lon-windw):(i_lon+windw), (i_lat-windw):(i_lat+windw), ] %>% 
                as.vector()
              
              
              if(any(is.na(p))){
                
                lev <- NA
                prob <- NA
                
              } else {
                
                params <- lmom::pelgev(lmom::samlmu(p))
                
                lev <- lmom::quagev(0.99,
                                    para = params)
                
                prob <- 1 - lmom::cdfgev(b,
                                         para = params)
                
              }
              
              res <- c(lev = lev,
                       prob = prob)
              
              return(res)
              
            })
          }) 
        
        
        wl_stats <- 
          map(c("lev", "prob"), function(colmn){
            
            s <- 
              wl_stats %>%
              pull(colmn) %>% 
              matrix(nrow = length(sub_vect_lon), ncol = length(sub_vect_lat), byrow = T) %>% 
              {cbind(NA, .)} %>% 
              {cbind(., NA)} %>% 
              {rbind(NA, .)} %>% 
              {rbind(., NA)} %>% 
              st_as_stars() %>% 
              setNames(colmn)
            
            st_dimensions(s) <- st_dimensions(swl %>% slice(time, 1))
            
            return(s)
            
          }) %>% 
          do.call(c, .)
        
        
        
      })
    
    hundred_yr_storm <- 
      c(list(baseline_stats), wl_stats)%>% 
      {do.call(c, c(., along = "wl"))} %>% 
      st_set_dimensions(3, values = seq(0.5, 3, 0.5))
    
    return(hundred_yr_storm)
    
  })





# PREPARE TO PLOT


hundred_yr_storm <- 
  hundred_yr_storm %>% 
  {do.call(c, c(., along = "models"))} 

land_pol <- 
  "/mnt/bucket_mine/misc_data/ne_110m_land/" %>% 
  st_read() %>%
  mutate(a = 1) %>% 
  select(a)

land_rast_0.1 <- 
  c(st_point(c(-180, -90)),
    st_point(c(180, 90))) %>%
  st_bbox() %>%
  st_set_crs(4326) %>% 
  st_as_stars(dx = 0.1, dy = 0.1, values = NA) %>% 
  {st_rasterize(land_pol, .)}

land_rast_0.2 <- 
  st_warp(land_rast_0.1, hundred_yr_storm %>% slice(wl, 1) %>% slice(models, 1))






# PLOT ***********************************


# AMOUNT *********************

hundred_yr_storm_lev <- 
  hundred_yr_storm %>% 
  select(1) %>% 
  st_apply(c(1,2,3), mean, na.rm = T, .fname = "lev")

hundred_yr_storm_lev[is.na(land_rast_0.2)] <- NA


# RAW
hundred_yr_storm_lev %>% 
  as_tibble() %>% 
  mutate(lev = raster::clamp(lev, 100, 700)) %>% 
  
  ggplot(aes(lon,lat, fill = lev)) +
  geom_raster() +
  facet_wrap(~wl, 3, 3) +
  colorspace::scale_fill_binned_sequential("plasma",
                                           rev = F,
                                           n.breaks = 7,
                                           na.value = "transparent") +
  coord_equal()


# CHANGE
hundred_yr_storm_lev %>%
  split("wl") %>%
  setNames(wls) %>% 
  as_tibble() %>%
  mutate(across(`1.0`:`3.0`, ~.x - `0.5`)) %>% 
  pivot_longer(`0.5`:`3.0`, names_to = "wl", values_to = "lev") %>% 
  mutate(lev = ifelse(wl == "0.5", NA, lev),
         lev = round(lev),
         lev = case_when(lev < -1 ~ "< -1",
                         lev >= -1 & lev <= 11 ~ "-1-+11",
                         lev >= 12 & lev <= 24 ~ "+12-+24",
                         lev >= 25 & lev <= 50 ~ "+25-+50",
                         lev > 50 ~ "> +50") %>% 
           factor(levels = c("< -1", "-1-+11", "+12-+24", "+25-+50", "> +50"))) %>% 
  
  
  ggplot(aes(lon,lat, fill = lev)) +
  geom_raster() +
  facet_wrap(~wl, 3, 3) +
  # colorspace::scale_fill_binned_divergingx("RdYlBu",
  #                                          rev = F,
  #                                          breaks = c(-1, 10, 25, 50),
  #                                          na.value = "transparent",
  #                                          mid = 2) +
  scale_fill_manual(values = pf_pal, na.value = "transparent", na.translate = F) +
  coord_equal() +
  theme(axis.title = element_blank()) +
  labs(title = "Change in Precipitation of 100 yr storm")



# FREQ *************************


hundred_yr_storm_prob <- 
  hundred_yr_storm %>% 
  select(2) %>% 
  st_apply(c(1,2,3), mean, na.rm = T, .fname = "prob")


hundred_yr_storm_prob[is.na(land_rast_0.2)] <- NA


# RAW
hundred_yr_storm_prob %>% 
  select(prob) %>% 
  as_tibble() %>% 
  mutate(prob = ifelse(wl == 0.5, NA, prob),
         prob = raster::clamp(prob, 0.01, 0.06)) %>%
  
  ggplot(aes(lon,lat, fill = prob)) +
  geom_raster() +
  facet_wrap(~wl, 3, 3) +
  colorspace::scale_fill_binned_sequential("plasma",
                                           rev = F,
                                           n.breaks = 7,
                                           na.value = "transparent") +
  coord_equal()



# CHANGE
hundred_yr_storm_prob %>% 
  split("wl") %>%
  setNames(wls) %>% 
  as_tibble() %>%
  mutate(across(`1.0`:`3.0`, ~.x/0.01)) %>% 
  pivot_longer(`0.5`:`3.0`, names_to = "wl", values_to = "prob") %>% 
  mutate(prob = ifelse(wl == "0.5", NA, prob),
         prob = round(prob),
         prob = case_when(prob < 1 ~ "< 1",
                          prob == 1 ~ "1",
                          prob == 2 ~ "2",
                          prob == 3 | prob == 4 ~ "3-4",
                          prob > 4 ~ "> 4") %>% 
           factor(levels = c("< 1", "1", "2", "3-4", "> 4"))) %>% 
  
  
  ggplot(aes(lon,lat, fill = prob)) +
  geom_raster() +
  facet_wrap(~wl, 3, 3) +
  # colorspace::scale_fill_binned_divergingx("RdYlBu",
  #                                          rev = F,
  #                                          breaks = c(0, 1, 2, 3, 4, 5),
  #                                          na.value = "transparent",
  #                                          mid = 1) +
  scale_fill_manual(values = pf_pal, na.value = "transparent", na.translate = F) +
  coord_equal() + 
  theme(axis.title = element_blank()) +
  labs(title = "Change in Frequency of 100 yr storm (X times +/- frequent)")







