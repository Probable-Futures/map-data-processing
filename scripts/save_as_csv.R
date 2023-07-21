
library(tidyverse)
library(stars)



dir_mosaicked <- "/mnt/bucket_mine/results/global_heat_pf/03_mosaicked/land/v3"
dir_csv <- "/mnt/bucket_mine/results/global_heat_pf/csv/land"


dir_mosaicked %>% 
  list.files() %>% 
  walk(function(f){
    
    print(f)
    
    s <- 
      str_glue("{dir_mosaicked}/{f}") %>% 
      read_ncdf() %>% 
      suppressMessages()
    
    tb_1 <- 
      s %>% 
      as_tibble(center = T) %>% 
      pivot_longer(-c(lon,lat,wl), names_to = "stat") %>% 
      filter(!is.na(value))
    
    tb_2 <- 
      tb_1 %>%
      mutate(wl = sprintf("%.1f", wl),
             stat = str_glue("{stat}_{wl}")) %>% 
      select(-wl) %>% 
      pivot_wider(lon:lat, names_from = stat, values_from = value)
    
    write_csv(tb_2,
              str_glue("{dir_csv}/{f %>% str_replace('.nc$', '.csv')}"))
    
    
  })
