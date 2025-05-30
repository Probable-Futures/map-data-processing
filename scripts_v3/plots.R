
library(tidyverse)
library(stars)
library(furrr)
library(ggridges)


plan(multisession)


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

wls <- c("0.5", "1.0", "1.5", "2.0", "2.5", "3.0")


fn_tb <- function(variable, lon, lat, make_t) {
  
  l_filenames <- 
    "/mnt/bucket_mine/results/global_heat_pf/01_derived" %>% 
    list.files(full.names = T) %>% 
    str_subset(variable) 
  
  s_proxy <- 
    read_ncdf(l_filenames[[1]], proxy = T, make_time = F)
  
  lon_coord <- 
    st_get_dimension_values(s_proxy, 1) %>% 
    {abs(. - lon)} %>% 
    which.min()
  
  lat_coord <- 
    st_get_dimension_values(s_proxy, 2) %>% 
    {abs(. - lat)} %>% 
    which.min()
  
  models <- 
    l_filenames %>% 
    str_split("/") %>% 
    map_chr(last) %>% 
    str_split("_") %>% 
    map_chr(last) %>% 
    str_remove(".nc")
  
  
  tb <- 
    future_map2(l_filenames, models, function(f, mod) {
      
      read_ncdf(f, ncsub = cbind(start = c(lon_coord, lat_coord, 1),
                                 count = c(1,1,NA)),
                make_time = make_t) %>%
        suppressMessages() %>% 
        
        setNames("v") %>% 
        
        as_tibble() %>% 
        mutate(model = mod,
               time = time %>% str_sub(end = 4) %>% as.integer())
      
    })
  
}



fn_slice <- function(tb, wl) {
  
  if(wl == "0.5"){
    
    tb %>% 
      filter(time >= 1971,
             time <= 2000)
    
    # other warming levels:
  } else {
    
    gcm <- tb$model[1]
    
    thres_val <-
      thresholds %>%
      filter(str_detect(Model, str_glue("{gcm}$"))) %>% 
      filter(wl == {{wl}})
    
    tb %>% 
      filter(time >= thres_val$value - 10,
             time <= thres_val$value + 10)
    
  }
  
}



tb_10hotdays <- fn_tb("NAM_ten-hottest-days-tasmax",
                      -112.08, 33.45,
                      make_t = F)

tb_10hotdays <- 
  tb_10hotdays %>% 
  map(mutate,
      v = v %>% 
        units::set_units(degC) %>% 
        units::set_units(NULL))



tb_10hotdays %>% 
  bind_rows() %>% 
  filter(time <= 2023) %>% 
  
  ggplot(aes(x = time, y = v, color = v)) +
  geom_point(size = 2) +
  geom_point(shape = 21, color = "black", size = 2) +
  colorspace::scale_color_continuous_sequential("plasma",
                                                rev = F,
                                                guide = "none") +
  labs(y = "Temperature (°C)",
       title = "10 hottest days",
       subtitle = "Phoenix, AZ") +
  
  geom_hline(yintercept = 43.3, linetype = "2222", color = "grey20") +
  geom_hline(yintercept = 47.7, linetype = "2222", color = "grey20") +
  
  theme(axis.title.x = element_blank())




map(wls, function(wl_){
  
  map_dfr(tb_10hotdays, fn_slice, wl = wl_) %>% 
    mutate(wl = wl_)
  
}) %>% 
  bind_rows() %>% 
  
  ggplot(aes(x = v, y = wl, fill = after_stat(x))) +
  geom_density_ridges_gradient(bandwidth = 0.6) +
  colorspace::scale_fill_continuous_sequential("plasma",
                                               rev = F,
                                               guide = "none") +
  labs(y = "Warming level",
       x = "Temperature (°C)",
       title = "Distribution of the 10 hottest days",
       subtitle = "Phoenix, AZ") +
  
  # geom_rect(aes(xmin = 43.3, xmax = 47.7, ymin = -Inf, ymax = Inf), 
  #           fill = "blue", alpha = 0.5)
  
  geom_vline(xintercept = 43.3, linetype = "2222", color = "grey20") +
  geom_vline(xintercept = 47.7, linetype = "2222", color = "grey20")




  







tb_90wetdays <- fn_tb("NAM_ninety-wettest-days",
                      -72.57, 44.26,
                      make_t = T)

tb_90wetdays <- 
  tb_90wetdays %>% 
  map(mutate,
      v = v %>% 
        units::set_units(kg/m^2/d) %>% 
        units::set_units(NULL))



tb_90wetdays %>% 
  bind_rows() %>% 
  filter(time <= 2023) %>% 
  
  ggplot(aes(x = time, y = v, color = v)) +
  geom_point(size = 2) +
  geom_point(shape = 21, color = "black", size = 2) +
  colorspace::scale_color_continuous_sequential("plasma",
                                                rev = F,
                                                guide = "none") +
  labs(y = "Precipitation (mm)",
       title = "90 wettest days",
       subtitle = "Montpellier, VT") +
  
  theme(axis.title.x = element_blank())



map(wls, function(wl_){
  
  map_dfr(tb_90wetdays, fn_slice, wl = wl_) %>% 
    mutate(wl = wl_)
  
}) %>% 
  bind_rows() %>% 
  
  ggplot(aes(x = v, y = wl, fill = after_stat(x))) +
  geom_density_ridges_gradient() +
  colorspace::scale_fill_continuous_sequential("plasma",
                                               rev = F,
                                               guide = "none") +
  labs(y = "Warming level",
       x = "Precipitation (mm)",
       title = "Distribution of the 90 wettest days",
       subtitle = "Montpellier, VT")



