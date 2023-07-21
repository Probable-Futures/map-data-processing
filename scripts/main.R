
var_input <- c("maximum_temperature",
               "minimum_temperature",
               "maximum_wetbulb_temperature",
               "average_temperature",
               
               "precipitation",
               "precipitation+average_temperature",
               "precipitation+maximum_temperature",
               
               "spei")[8]


source("scripts/setup.R")
source("scripts/functions.R")
# source("scripts/cdo_proc.R")

plan(multicore)


dir_derived <- 
  str_glue("{dir_bucket_mine}/results/global_heat_pf/01_derived")

derived_vars <- 
  tb_vars %>% 
  filter(var_input == {{var_input}}) %>% 
  pull(var_derived)

# derived_vars = derived_vars[1]

# derived_var = derived_vars[3] # *****

doms <- c("SEA", "AUS", "CAS", "WAS", "EAS", "AFR", "EUR", "NAM", "CAM", "SAM")

# doms = doms[-7] # *****

wl <- c("0.5", "1.0", "1.5", "2.0", "2.5", "3.0")



source("scripts/01_derived.R")
source("scripts/02_ensemble.R")
source("scripts/03_mosaic.R")






# dir_derived %>%
#   list.files(full.names = T) %>%
#   # str_subset("AFR") %>%
#   str_subset("tasmin") %>%
#   walk(file.remove)





















# TILING ------------------------------------------------------------------------------------------

f <-
  dir_remo %>%
  list.files(full.names = T) %>%
  str_subset("_2000.nc") %>% 
  str_subset("Had") %>% 
  .[1]

source("scripts/tiling.R")



# LOOP THROUGH TILES ------------------------------------------------------------------------------

calendar_type <- 
  tb_models %>% 
  filter(gcm == gcm_,
         rcm == rcm_ %>% str_split("-", simplify = T) %>% .[,2]) %>% 
  pull(calendar)



for(i in seq_len(nrow(chunks_ind))){
  
  i <- 24
  
  
  print(str_glue(" "))
  print(str_glue("*******************"))
  print(str_glue("PROCESSING TILE {i}"))
  
  r <- chunks_ind$r[i]
  lon_ch <- chunks_ind$lon_ch[i]
  lat_ch <- chunks_ind$lat_ch[i]
  
  
  
  ## READ DATA -------------------------------------------------------------------------------------
  
  ncs <- 
    cbind(start = c(lon_chunks[[lon_ch]][1], lat_chunks[[lat_ch]][1], 1),
          count = c(lon_chunks[[lon_ch]][2] - lon_chunks[[lon_ch]][1]+1,
                    lat_chunks[[lat_ch]][2] - lat_chunks[[lat_ch]][1]+1,
                    NA))
  
  
  s <- 
    dir_raw_data %>% 
    list.files(full.names = T) %>% 
    
    future_map(function(f){
      
      time_dim <- 
        f %>% 
        read_ncdf(proxy = T) %>% 
        suppressMessages() %>% 
        st_get_dimension_values("time") %>% 
        as.character()
      
      fixed_time <- 
        fn_dates(time_dim, calendar_type)
      
      # import and fix time dim
      str_c(f) %>% 
        read_ncdf(ncsub = ncs) %>% 
        suppressMessages() %>% 
        st_set_dimensions("time", values = fixed_time) -> s 
      
      # regularize grid
      s <-
        s %>%
        st_set_dimensions("lon", values = st_get_dimension_values(s, "lon") %>% round(1)) %>% 
        st_set_dimensions("lat", values = st_get_dimension_values(s, "lat") %>% round(1)) %>% 
        st_set_crs(4326)
      
    },
    .options = furrr_options(seed = NULL)
    ) %>% 
    do.call(c, .)
  
  
  
  