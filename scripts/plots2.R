
library(tidyverse)
library(stars)
library(furrr)
library(lubridate)

plan(multisession)



dir_data <- "/mnt/pers_disk/data"
# fs::dir_create(dir_data)
# 
# # download
# "/mnt/bucket_cmip5/RCM_regridded_data/REMO2015/NAM/daily/precipitation/" %>% 
#   fs::dir_ls(regexp = "\\d{8}-\\d{8}") %>% 
#   future_walk(function(f) {
#     
#     f_gs <- f %>% str_replace("/mnt/bucket_cmip5", "gs://cmip5_data")
#     
#     str_glue("gsutil cp {f_gs} {d}", d = dir_data) %>% 
#       system(ignore.stdout = T, ignore.stderr = T)
#     
  # })


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


l_filenames <- 
    dir_data %>% 
    list.files(full.names = T)
  
s_proxy <- 
  read_ncdf(l_filenames[[1]], proxy = T, make_time = F)

lon_coord <- 
  st_get_dimension_values(s_proxy, 1) %>% 
  {abs(. - -72.57)} %>% 
  which.min()

lat_coord <- 
  st_get_dimension_values(s_proxy, 2) %>% 
  {abs(. - 44.26)} %>% 
  which.min()

models <- 
  l_filenames %>% 
  str_split("/") %>% 
  map_chr(last) %>% 
  str_split("_") %>% 
  map_chr(~.x[[3]]) %>% 
  str_remove(".nc")

  
tb <- 
  future_map2_dfr(l_filenames, models, function(f, mod) {
    
    read_ncdf(f, ncsub = cbind(start = c(lon_coord, lat_coord, 1),
                               count = c(1,1,NA)),
              make_time = T) %>%
      suppressMessages() %>% 
      
      setNames("v") %>%
      
      mutate(v = v %>% units::set_units(kg/m^2/d) %>% units::set_units(NULL)) %>% 
      
      as_tibble() %>%
      mutate(model = mod,
             #time = time %>% str_sub(end = 4) %>% as.integer()
             # time = str_sub(time, end = 8) %>% {str_glue("{.}01")} %>% lubridate::as_date()
             time = str_sub(time, end = 10)
             )
    
  })

# tb <- 
#   tb %>% 
#   mutate(yr = year(time))


# tb_m <- 
#   tb %>% 
#   group_by(mon = month(time), yr = year(time), model) %>% 
#   summarize(v = max(v))


# tb_ens <- 
#   
#   tb %>% 
#   mutate(yr = str_sub(time, end = 4) %>% as.numeric(),
#          mon = str_sub(time, start = 6, end = 7) %>% as.numeric(),
#          dy = str_sub(time, start = 9, end = 10) %>% as.numeric()) %>% 
#   group_by(yr, mon, dy) %>% 
#   summarize(v = mean(v), .groups = "drop")



tb_sliced <- 
  
  tb %>% 
  mutate(yr = str_sub(time, end = 4) %>% as.numeric()) %>% 
  group_by(model) %>% 
  group_split() %>% 
  
  map(function(tbb) {
    
    map(wls, function(wl) {
      
      if(wl == "0.5"){
        
        tbb %>% 
          filter(yr >= 1971,
                 yr <= 2000) %>% 
          mutate(wl = {{wl}})
        
        # other warming levels:
      } else {
        
        gcm <- tbb$model[1]
        
        thres_val <-
          thresholds %>%
          filter(str_detect(Model, str_glue("{gcm}$"))) %>% 
          filter(wl == {{wl}})
        
        tbb %>% 
          filter(yr >= thres_val$value - 10,
                 yr <= thres_val$value + 10) %>% 
          mutate(wl = {{wl}})
        
      }
    })
  }) %>% 
  
  transpose() %>%
  map(bind_rows)


    
# tb_stats <- 
#   tb_sliced[[1]] %>% 
#   group_by(yr, model) %>% 
#   summarize(q = max(v), .groups = "drop") %>%
#   group_by(model) %>% 
#   reframe(q = quantile(q, c(0.5, 0.9, 0.98, 0.99))) %>% 
#   mutate(qn = rep(c(50, 10, 2, 1), 3)) %>% 
#   group_by(qn) %>% 
#   summarize(q = mean(q))

tb_stats <- 
  tb_sliced[[1]] %>% 
  group_by(yr) %>% 
  summarize(q = max(v), .groups = "drop") %>%
  reframe(q = quantile(q, c(0.5, 0.9, 0.98, 0.99))) %>% 
  mutate(qn = c(50, 10, 2, 1))

# sum(tb_sliced[[1]]$v > tb_stats$q[1])


tb_1 = tb_sliced[[1]]

tb_1 %>% 
  mutate(
    # a = case_when(v > tb_stats$q[3] ~ 1,
    #               v > tb_stats$q[2] ~ 0.8,
    #               v > tb_stats$q[1] ~ 0.5,
    #               TRUE ~ 0.35)
    # a = ecdf(v)(v),
    # a = exp(a)^40,
    # a = a %>% scales::rescale(to = c(0,1))
    a = v/max(v)
  ) %>% 
  
  ggplot() +
  geom_jitter(aes(yr, v, color = v, alpha = a), width = 0.5, height = 0, show.legend = F) +
  colorspace::scale_color_continuous_sequential("viridis",
                                                begin = 0.35,
                                                trans = "sqrt") +
  geom_hline(yintercept = tb_stats$q, lty = "2222", color = "grey40") +
  theme(axis.title.x = element_blank()) +
  geom_text(data = tb_stats, aes(x = 2001, y = (q+3), label = str_glue("{qn}%"))) +
  scale_x_continuous(breaks = seq(1970,2001, by = 5), minor_breaks = seq(1970,2001)) +
  labs(title = "Daily rainfall in Montpellier",
       subtitle = "WL = 0.5",
       y = "mm") +
  scale_y_continuous(limits = c(0,165))






tb_sliced[[6]] %>%
  group_by(model) %>% 
  mutate(yr = scales::rescale(yr, to = c(1,20))) %>% 
  mutate(
    a = v/max(v)
  ) %>% 
  
  ggplot() +
  geom_jitter(aes(yr, v, color = v, alpha = a), width = 0.5, height = 0, show.legend = F) +
  colorspace::scale_color_continuous_sequential("viridis",
                                                begin = 0.35,
                                                trans = "sqrt") +
  geom_hline(yintercept = tb_stats$q, lty = "2222", color = "grey40") +
  theme(axis.title.x = element_blank()) +
  geom_text(data = tb_stats, aes(x = 20, y = (q+3), label = str_glue("{qn}%"))) +
  scale_x_continuous(breaks = c(1, seq(5,20, by = 5)), minor_breaks = seq(0,20)) +
  labs(title = " ",
       subtitle = "WL = 3.0",
       y = "mm") +
  scale_y_continuous(limits = c(0,165))

