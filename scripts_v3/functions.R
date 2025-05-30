
# Function to assemble a table of all CORDEX files needed to calculate
# a derived variable. Columns of table include the name of the file, 
# climate variable, GCM, RCM, period of time, and location of the file.
# File naming differs among wetbulb, SPEI, and the rest of variables, so
# extraction of info for each is different.


fn_data_table <- function(vari){
  
  if(str_detect(vari, "spei")){
    t_res <- "monthly"
  } else {
    t_res <- "daily"
  }
  
  
  dir_remo <- str_glue("{dir_cordex}/REMO2015/{dom}/{t_res}/{vari}")
  dir_regcm <- str_glue("{dir_cordex}/CORDEX_22/{dom}/{t_res}/{vari}")
  
  
  tb_files <- 
    map_dfr(c(dir_remo, dir_regcm), function(dd){
      
      if(str_detect(dd, "REMO2015")){
        rcm_ = "REMO2015"
        i = 0
      } else if(str_detect(dd, "CORDEX_22")){
        rcm_ = "RegCM4"
        i = 1
      }
      
      
      if(str_detect(vari, "wetbulb")){
        
        dd %>% 
          list.files() %>%
          str_subset(".nc$") %>% 
          
          map_dfr(function(d){
            
            tibble(file = d) %>%
              
              mutate(
                
                var = "maxwetbulb",
                
                gcm = file %>%
                  str_split("_", simplify = T) %>%
                  .[ , i+3],
                
                rcm = rcm_,
                
                t_i = file %>%
                  str_split("_", simplify = T) %>%
                  .[ , i+4] %>%
                  str_sub(end = 4) %>% 
                  str_c("0101"),
                
                t_f = file %>%
                  str_split("_", simplify = T) %>%
                  .[ , i+4] %>%
                  str_sub(end = 4) %>% 
                  str_c("1201"),
                
                loc = dd
                
              )
          }) 
        
        
        
      } else if(str_detect(vari, "spei")){
        
        dd %>% 
          list.files() %>%
          str_subset(".nc$") %>%
          str_subset(".*_[:digit:]*-[:digit:]*.nc") %>%
          
          map_dfr(function(d){
            
            tibble(file = d) %>%
              
              mutate(
                
                var = file %>%
                  str_split("_", simplify = T) %>%
                  .[ , 1],
                
                gcm = file %>%
                  str_split("_", simplify = T) %>%
                  .[ , 4],
                
                rcm = rcm_,
                
                t_i = file %>%
                  str_split("_", simplify = T) %>%
                  .[ , 6] %>%
                  str_sub(end = 6) %>% 
                  str_c("01"),
                
                t_f = file %>%
                  str_split("_", simplify = T) %>%
                  .[ , 6] %>%
                  str_sub(start = 10, end = 15) %>% 
                  str_c("01"),
                
                loc = dd
                
              )
          }) 
        
        
        
      } else if(str_detect(vari, "fire")){
        
        dd %>% 
          list.files() %>%
          str_subset(".nc$") %>%
          str_subset(".*_[:digit:]*-[:digit:]*.nc") %>%
          
          map_dfr(function(d){
            
            tibble(file = d) %>%
              
              mutate(
                
                var = "fwi",
                
                gcm = file %>%
                  str_split("_", simplify = T) %>%
                  .[ , i+3],
                
                rcm = rcm_,
                
                t_i = file %>%
                  str_split("_", simplify = T) %>%
                  .[ , 6] %>%
                  str_sub(end = 6) %>% 
                  str_c("01"),
                
                t_f = file %>%
                  str_split("_", simplify = T) %>%
                  .[ , 6] %>%
                  str_sub(start = 10, end = 15) %>% 
                  str_c("01"),
                
                loc = dd
                
              )
          })
        
      } else {
        
        dd %>% 
          list.files() %>%
          str_subset(".nc$") %>%
          str_subset(".*_[:digit:]*-[:digit:]*.nc") %>%
          
          map_dfr(function(d){
            
            tibble(file = d) %>%
              
              mutate(
                
                var = file %>%
                  str_split("_", simplify = T) %>%
                  .[ , 1],
                
                gcm = file %>%
                  str_split("_", simplify = T) %>%
                  .[ , 3],
                
                rcm = rcm_,
                
                t_i = file %>%
                  str_split("_", simplify = T) %>%
                  .[ , 9] %>%
                  str_sub(end = 6) %>% 
                  str_c("01"),
                
                t_f = file %>%
                  str_split("_", simplify = T) %>%
                  .[ , 9] %>%
                  str_sub(start = 10, end = 15) %>% 
                  str_c("01"),
                
                loc = dd
                
              )
          }) 
      }
      
      
    }) %>% 
    
    filter(year(as_date(t_i)) >= 1970) %>% 
    filter(
      str_detect(gcm, "EC-EARTH", negate = T),
      str_detect(gcm, "CERFACS", negate = T),
      str_detect(gcm, "CNRM", negate = T)
    )
  
  
  # some precip files for AFR (REMO Had model) 
  # have a different naming structure:
  
  if(dom == "AFR" & vari == "precipitation"){
    
    tb_files <- 
      
      dir_remo %>% 
      list.files() %>%
      str_subset(".nc$") %>% 
      str_subset("REMO") %>% 
      str_subset("HadGEM2") %>% 
      str_subset("historical") %>%
      
      map_dfr(function(d){
        
        tibble(file = d) %>%
          
          mutate(
            
            var = file %>%
              str_split("_", simplify = T) %>%
              .[ , 1],
            
            gcm = file %>%
              str_split("_", simplify = T) %>%
              .[ , 3],
            
            rcm = "REMO2015",
            
            t_i = file %>%
              str_split("_", simplify = T) %>%
              .[ , 9] %>%
              str_sub(end = 4) %>% 
              str_c("0101"),
            
            t_f = file %>%
              str_split("_", simplify = T) %>%
              .[ , 9] %>%
              str_sub(end = 4) %>% 
              str_c("1201"),
            
            loc = dir_remo
            
          )
      }) %>% 
      
      bind_rows(tb_files)
    
    
  }
  
  return(tb_files)
}



# *****************



fn_tiling <- function(proxy_map){
  
  # size of tile (approximate number of cells in each dim)
  sz <- 50
  
  
  # obtain chunks of longitude
  
  d_lon <- 
    proxy_map %>% 
    dim() %>% 
    .[1] %>% 
    seq_len()
  
  n_lon <- 
    round(length(d_lon)/sz)
  
  lon_chunks <- 
    split(d_lon, 
          ceiling(seq_along(d_lon)/(length(d_lon)/n_lon))) %>% 
    map(~c(first(.x), last(.x)))
  
  
  # obtain chunks of latitude
  
  d_lat <- 
    proxy_map %>% 
    dim() %>% 
    .[2] %>% 
    seq_len()
  
  n_lat <- 
    round(length(d_lat)/sz)
  
  lat_chunks <- 
    split(d_lat, 
          ceiling(seq_along(d_lat)/(length(d_lat)/n_lat))) %>% 
    map(~c(first(.x), last(.x)))
  
  
  # results
  return(list(lon_chunks = lon_chunks,
              lat_chunks = lat_chunks))
  
  
}























# Wrapper of the function to calculate derived variables (fn_derived), with 
# added functionality to overwrite results and verify correct time dimension.
# This function was written so that variables could be selectively processed
# in parallel.


# fn_fn_derived <- function(derived_var){
#   
#   outfile <-
#     str_glue("{dir_derived}/{dom}_{derived_var}_yr_{rcm_}_{gcm_}.nc")
#   
#   if(file.exists(outfile)){
#     file.remove(outfile)
#     print(str_glue("      (old derived file removed)"))
#   }
#   
#   # call function to calculate drived var
#   fn_derived(derived_var, outfile)
#   
#   # verify correct time dimension
#   time_steps <-
#     str_glue("{dir_derived}/{dom}_{derived_var}_yr_{rcm_}_{gcm_}.nc") %>%
#     read_ncdf(proxy = T, make_time = F) %>%
#     suppressMessages() %>%
#     suppressWarnings() %>%
#     st_get_dimension_values("time") %>%
#     length()
#   
#   print(str_glue("      Done: new file with {time_steps} timesteps ({derived_var})"))
#   
# }







# fn_models_table <- function(tb_files){
#   
#   tb_models <- 
#     unique(tb_files[, c("gcm", "rcm")]) %>% 
#     mutate(calendar = case_when(str_detect(gcm, "Had") ~ 360,
#                                 str_detect(rcm, "REMO") & str_detect(gcm, "Had", negate = T) ~ 365.25,
#                                 str_detect(rcm, "RegCM") & str_detect(gcm, "MPI") ~ 365.25,
#                                 str_detect(rcm, "RegCM") & str_detect(gcm, "Nor") ~ 365,
#                                 str_detect(rcm, "RegCM") & str_detect(gcm, "GFDL") ~ 365))
#   
#   
#   if(dom %in% c("SAM", "AUS", "CAS")){
#     
#     tb_models <- 
#       tb_models %>% 
#       filter(str_detect(rcm, "RegCM", negate = T))
#     
#   }
#   
#   return(tb_models)
# }


# ************










# fn_dates <- function(d, cal_type){
#   
#   if(cal_type == 360){
#     
#     fixed_time <- 
#       
#       tibble(time = str_sub(d, end = 10),
#            yr = str_sub(time, end = 4),
#            mon = str_sub(time, start = 6, end = 7)) %>% 
#       
#       group_by(yr, mon) %>% 
#       nest() %>% 
#       mutate(dy = map(data, function(df){
#         
#         seq(1,
#             days_in_month(df$time[1]),
#             length.out = 30) %>% 
#           round()
#         
#       })) %>% 
#       unnest(c(dy, data)) %>% 
#       mutate(time = str_glue("{yr}-{mon}-{dy}") %>% as_date()) %>% 
#       pull(time)
#     
#   } else if(cal_type == 365){
#     
#     fixed_time <- 
#       
#       tibble(time = str_sub(d, end = 10),
#              yr = str_sub(time, end = 4)) %>% 
#       
#       group_by(yr) %>% 
#       nest() %>% 
#       mutate(time = map(data, function(df){
#         
#         seq(df$time %>% first() %>% as_date(),
#             df$time %>% last() %>% as_date(),
#             length.out = 365)
#         
#       })) %>% 
#       unnest(time) %>% 
#       pull(time)
#       
#     
#   } else if(cal_type == 365.25){
#     
#     fixed_time <- 
#       seq(as_date(first(d)), as_date(last(d)), by = "1 day")
#     
#   }
#   
#   return(fixed_time)
#   
# }



# *****************



# fn_statistics <- function(ts){
#   
#   if(any(is.na(ts))){
#     
#     c(mean = NA,
#       perc05 = NA, 
#       perc50 = NA,
#       perc95 = NA)
#     
#   } else {
#     
#     c(mean = mean(ts),
#       quantile(ts, c(0.05, 0.5, 0.95)) %>% setNames(c("perc05", "perc50", "perc95")))
#     
#   }
# }



# *****************




fn_write_nc <- function(star_to_export, filename, name_3rd_dim, un_3rd_dim = "", un = ""){
  
  if(file.exists(filename)){
    file.remove(filename)
    print(str_glue("\t (previous ensemble deleted)"))
  }
  
  
  # define dimensions
  dim_lon <- ncdf4::ncdim_def(name = "lon", 
                              units = "degrees_east", 
                              vals = star_to_export %>% 
                                st_get_dimension_values(1) %>% 
                                round(1))
  
  dim_lat <- ncdf4::ncdim_def(name = "lat", 
                              units = "degrees_north", 
                              vals = star_to_export %>% 
                                st_get_dimension_values(2) %>% 
                                round(1))
  
  dim_time <- ncdf4::ncdim_def(name = name_3rd_dim, 
                               units = un_3rd_dim, 
                               vals = star_to_export %>% 
                                 st_get_dimension_values(3))
  
  # define variables
  varis <- 
    names(star_to_export) %>% 
    map(~ncdf4::ncvar_def(name = all_of(.x),
                          units = un,
                          dim = list(dim_lon, dim_lat, dim_time), 
                          missval = -999999))
  
  
  # create file
  ncnew <- ncdf4::nc_create(filename = filename, 
                            vars = varis,
                            force_v4 = TRUE)
  
  # write data
  seq_along(names(star_to_export)) %>% 
    walk(~ncdf4::ncvar_put(nc = ncnew, 
                           varid = varis[[.x]], 
                           vals = star_to_export %>% select(.x) %>% pull(1)))
  
  ncdf4::nc_close(ncnew)
  
}




