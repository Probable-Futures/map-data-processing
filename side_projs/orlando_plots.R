
library(tidyverse)
library(stars)
library(furrr)
library(units)
library(colorspace)

options(future.fork.enable = T)
plan(multicore)


fn_get_cell_pos <- 
  
  function(star_obj, dim_id, coord) {
    
    star_obj %>% 
      st_get_dimension_values(dim_id) %>% 
      {. - coord} %>% 
      abs() %>% 
      which.min()
    
  }



varr <- 
  "maximum_wetbulb_temperature"                                           # *********
# "maximum_temperature"


dir_cordex <- "/mnt/bucket_cmip5/RCM_regridded_data"
dir_raw_data <- "/mnt/pers_disk/data/"
dom <- "NAM"

tb_files <- fn_data_table(varr) # function from main set of scripts functions.R

tb_models <-
  unique(tb_files[, c("gcm", "rcm")]) %>% 
  arrange(rcm, gcm)



# load thresholds table
thresholds <- 
  "../../map-data-processing/cmip5_model_temp_thresholds.csv" %>% 
  read_delim() %>%
  suppressMessages() %>% 
  select(1:6) %>% 
  pivot_longer(-Model, names_to = "wl") %>% 
  
  mutate(wl = str_sub(wl, 3)) %>% 
  mutate(wl = ifelse(str_length(wl) == 1, str_glue("{wl}.0"), wl))  %>%
  
  # add institutes to model names (for joins to work)
  mutate(Model = case_when(str_detect(Model, "HadGEM") ~ str_glue("MOHC-{Model}"),
                           str_detect(Model, "MPI") ~ str_glue("MPI-M-{Model}"),
                           str_detect(Model, "NorESM") ~ str_glue("NCC-{Model}"),
                           str_detect(Model, "GFDL") ~ str_glue("NOAA-GFDL-{Model}"),
                           str_detect(Model, "MIROC") ~ str_glue("MIROC-{Model}"),
                           TRUE ~ Model))




# MODEL LOOP 

l_tb <- vector("list", nrow(tb_models))

for (mod in seq_len(nrow(tb_models))) {
  
  gcm <- tb_models$gcm[mod]
  rcm <- tb_models$rcm[mod]
  
  
  # DOWNLOAD
  tb_files %>%
    filter(gcm == {{gcm}}, rcm == {{rcm}}) %>% 
    future_pwalk(function(loc, file, ...){
      
      loc_ <-
        loc %>% 
        str_replace("/mnt/bucket_cmip5", "gs://cmip5_data")
      
      str_glue("{loc_}/{file}") %>%
        {system(str_glue("gsutil cp {.} {dir_raw_data}"), 
                ignore.stdout = T, ignore.stderr = T)}
      
    })
  
  
  # IDENTIFY ORLANDO'S CELL POSITION
  s_proxy <- 
    dir_raw_data %>% 
    list.files(full.names = T) %>% 
    first() %>% 
    read_ncdf(ncsub = cbind(start = c(1,1,1),
                            count = c(NA,NA,1))) %>% 
    adrop() %>% 
    suppressMessages()
  
  pos_lon <- fn_get_cell_pos(s_proxy, 1, -81.3755)
  pos_lat <- fn_get_cell_pos(s_proxy, 2, 28.5337)
  
  
  # LOAD DATA
  tb_ts <- 
    dir_raw_data %>% 
    list.files(full.names = T) %>% 
    future_map_dfr(function(f) {
      
      s <- 
        read_ncdf(f, ncsub = cbind(start = c(pos_lon, pos_lat, 1),
                                   count = c(1,1,NA))) %>%
        suppressMessages() %>% 
        setNames("v")
      
      if (varr == "maximum_temperature") {
        
        s <- 
          s %>% 
          mutate(v = units::set_units(v, degC)) %>% 
          as_tibble()
        
      } else if (str_detect(varr, "wetbulb")) {
        
        yr <- 
          f %>% 
          str_sub(start = -7, end = -4)
        
        s <- 
          s %>% 
          as_tibble() %>% 
          mutate(time = as_date(str_glue("{yr}-01-01")))
        
        
      }
      
      s %>% 
        select(-c(1,2))
      
    })
  
  
  # SLICE WARMING LEVELS
  l_ts_wl <- 
    
    # loop through warming levels
    map_dfr(c("0.5", "3.0"), function(wl){
      
      if (wl == "0.5") {
        
        tb <- 
          tb_ts %>% 
          filter(year(time) >= 1970,
                 year(time) <= 2000)
        
      } else {
        
        thres_val <-
          thresholds %>%
          filter(str_detect(Model, str_glue("{gcm}$"))) %>% 
          filter(wl == {{wl}})
        
        tb <- 
          tb_ts %>% 
          filter(year(time) >= thres_val$value - 10,
                 year(time) <= thres_val$value + 10)
        
        # verify correct slicing:
        print(str_glue("   {gcm}: {thres_val$Model}: {thres_val$value}"))
        
      }
      
      tb <- 
        tb %>% 
        mutate(wl = {{wl}},
               gcm = {{gcm}},
               rcm = {{rcm}})
      
      return(tb)
      
    }) %>% 
    suppressWarnings()
  
  
  l_tb[[mod]] <- l_ts_wl
  
  
  # DELETE FILES
  dir_raw_data %>% 
    list.files(full.names = T) %>% 
    future_walk(file.remove)
  
}


l_tb %>% 
  map(mutate, time = str_sub(time, end = 10)) %>% 
  bind_rows() %>% 
  units::drop_units() -> tb


saveRDS(tb, str_glue("/mnt/pers_disk/pf_orlando/tb_{varr}.rds"))

tb <- readRDS(str_glue("/mnt/pers_disk/pf_orlando/tb_{varr}.rds"))








tb %>%
  mutate(wl = ifelse(wl == "0.5", "0.5 °C", "3.0 °C")) %>%
  filter(str_sub(time, 6,7) %in% c("06","07","08")) %>% 
  ggplot(aes(x = v, fill = wl)) +
  geom_density(color = NA, alpha = 0.75, bw = 0.01) +
  scale_fill_manual(values = c("#5b95cc", "#fd7a7c")) +
  theme_classic() +
  coord_cartesian(expand = F) +
  theme(#axis.ticks.y = element_blank(),
    #axis.text.y = element_blank(),
    axis.title.x = element_blank(),
    legend.title = element_blank(),
    legend.position = c(1.01, 0.9),
    plot.margin = unit(c(1,1.8,0.8,1), "cm"),
    text = element_text(size = 15),
    axis.text.x = element_text(vjust = -0.7)) +
  # scale_x_continuous(breaks = seq(0,40,10), labels = str_glue("{seq(0,40,10)} °C")) +
  scale_x_continuous(limits = c(21, 44),
                     breaks = seq(22,42), 
                     labels = c("22°C", rep("",3), "26°C", rep("",3), "30°C", rep("",3), "34°C", rep("",3), "38°C", rep("",3), "42°C")) +
  scale_y_continuous(breaks = 0) +
  labs(y = "Frequency")



# set.seed(111)  
tb %>% 
  group_by(gcm,rcm, wl, yr = year(as_date(time))) %>% 
  summarize(v = sum(v > 28)) %>%
  # slice_sample(n = 21) %>%
  ungroup() %>% 
  mutate(wl = ifelse(wl == "0.5", "0.5 °C", "3.0 °C")) %>% 
  
  mutate(vv = case_when(v < 1 ~ "0",
                        v <= 7 ~ "1-7",
                        v <= 14 ~ "7-14",
                        v <= 30 ~ "14-30",
                        v <= 90 ~ "30-90",
                        TRUE ~ "90-180") %>% 
           factor(levels = c("0",
                             "1-7",
                             "7-14",
                             "14-30",
                             "30-90",
                             "90-180"))) %>% 
  
  complete(vv, wl) %>%
  
  ggplot(aes(x = vv, fill = wl)) +
  
  # geom_bar(position = position_dodge2(preserve = "single", padding = 0, width = 0.9)) +
  geom_bar(position = "dodge", width = 0.8) +
  
  # geom_histogram(position = "identity", alpha = 0.75, binwidth = 3) +
  scale_y_continuous(limits = c(0, 150), breaks = c(0,150), labels = c("Never", "Every\nyear")) +
  
  # geom_density(color = NA, alpha = 0.75, bw = 2) +
  # scale_y_continuous(limits = c(0, 0.2), breaks = c(0, 0.2), labels = c("Never", "Every\nyear")) +
  
  scale_fill_manual(values = c("#5b95cc", "#fd7a7c")) +
  # scale_x_discrete(drop = F) +
  
  theme_classic() +
  coord_cartesian(expand = F) +
  theme(#axis.ticks.y = element_blank(),
    #axis.text.y = element_blank(),
    axis.title.y = element_blank(),
    legend.title = element_blank(),
    legend.position = c(1.05, 0.9),
    plot.margin = unit(c(1,1.8,0.8,1), "cm"),
    text = element_text(size = 15),
    axis.text.x = element_text(vjust = -0.7)) +
  
  labs(x = "\ndays")
#scale_x_continuous(breaks = seq(0,40,10), labels = str_glue("{seq(0,40,10)} °C"))



tb %>% 
  group_by(gcm,rcm, wl, yr = year(as_date(time))) %>% 
  summarize(v = sum(v > 28)) %>%
  ungroup() %>% 
  mutate(wl = ifelse(wl == "0.5", "0.5 °C", "3.0 °C")) %>% 
  
  mutate(vv = case_when(v < 1 ~ "0",
                        v <= 7 ~ "1-7",
                        v <= 14 ~ "7-14",
                        v <= 30 ~ "14-30",
                        v <= 90 ~ "30-90",
                        TRUE ~ "90-180") %>% 
           factor(levels = c("0",
                             "1-7",
                             "7-14",
                             "14-30",
                             "30-90",
                             "90-180"))) %>% 
  
  complete(vv, wl) %>%
  
  ggplot(aes(y = vv, fill = wl)) +
  
  geom_bar(position = "dodge", width = 0.8) +
  
  scale_y_continuous(limits = c(0, 150), breaks = c(0,150), labels = c("Never", "Every\nyear")) +
  
  # geom_density(color = NA, alpha = 0.75, bw = 2) +
  # scale_y_continuous(limits = c(0, 0.2), breaks = c(0, 0.2), labels = c("Never", "Every\nyear")) +
  
  scale_fill_manual(values = c("#5b95cc", "#fd7a7c")) +
  # scale_x_discrete(drop = F) +
  
  theme_classic() +
  coord_cartesian(expand = F) +
  theme(#axis.ticks.y = element_blank(),
    #axis.text.y = element_blank(),
    axis.title.y = element_blank(),
    legend.title = element_blank(),
    legend.position = c(1.05, 0.9),
    plot.margin = unit(c(1,1.8,0.8,1), "cm"),
    text = element_text(size = 15),
    axis.text.x = element_text(vjust = -0.7)) +
  
  labs(x = "\ndays")





# set.seed(111)  
tb %>% 
  group_by(gcm,rcm, wl, yr = year(as_date(time))) %>% 
  summarize(v = sum(v > 28)) %>% #write_csv("/mnt/bucket_mine/misc_data/temporary/pf_wb.csv")
  # slice_sample(n = 21) %>%
  ungroup() %>% 
  mutate(wl = ifelse(wl == "0.5", "0.5 °C", "3.0 °C")) %>% 
  
  mutate(vv = case_when(v < 1 ~ "0",
                        v <= 7 ~ "1-7",
                        v <= 14 ~ "7-14",
                        v <= 30 ~ "14-30",
                        v <= 90 ~ "30-90",
                        TRUE ~ "90-180") %>% 
           factor(levels = c("0",
                             "1-7",
                             "7-14",
                             "14-30",
                             "30-90",
                             "90-180"))) %>% 
  
  group_by(wl, vv, .drop = F) %>% 
  summarize(n = n()) %>% 
  mutate(perc = n/sum(n)*100) %>% 
  
  ggplot(aes(y = vv, x = perc, fill = wl)) +
  
  geom_col(width = 0.8, position = "dodge") +
  
  # geom_histogram(position = "identity", alpha = 0.75, binwidth = 3) +
  scale_x_continuous(trans = "log", limits = c(1, 100), breaks = c(1, 5, 10, 50, 100), labels = str_glue("{c(1, 5, 10, 50, 100)}%")) +
  
  # geom_density(color = NA, alpha = 0.75, bw = 2) +
  # scale_y_continuous(limits = c(0, 0.2), breaks = c(0, 0.2), labels = c("Never", "Every\nyear")) +
  
  scale_fill_manual(values = c("#5b95cc", "#fd7a7c")) +
  
  theme_classic() +
  coord_cartesian(expand = F) +
  theme(#axis.ticks.y = element_blank(),
    #axis.text.y = element_blank(),
    axis.title.y = element_blank(),
    legend.title = element_blank(),
    legend.position = c(1.01, 0.9),
    plot.margin = unit(c(1,1.8,0.8,1), "cm"),
    text = element_text(size = 15),
    axis.text.x = element_text(vjust = -0.7)) +
  
  labs(x = "\ndays")
#scale_x_continuous(breaks = seq(0,40,10), labels = str_glue("{seq(0,40,10)} °C"))



tb %>% 
  group_by(gcm,rcm, wl, yr = year(as_date(time))) %>% 
  summarize(v = sum(v > 28)) %>%
  # slice_sample(n = 21) %>%
  ungroup() %>% 
  mutate(wl = ifelse(wl == "0.5", "0.5 °C", "3.0 °C")) %>% 
  
  mutate(vv = case_when(v < 1 ~ "0",
                        v <= 7 ~ "1-7",
                        v <= 14 ~ "7-14",
                        v <= 30 ~ "14-30",
                        v <= 90 ~ "30-90",
                        TRUE ~ "90-180") %>% 
           factor(levels = c("0",
                             "1-7",
                             "7-14",
                             "14-30",
                             "30-90",
                             "90-180"))) %>% 
  
  group_by(wl, vv, .drop = F) %>% 
  summarize(n = n()) %>% 
  mutate(perc = n/sum(n)*100) %>% 
  
  ggplot(aes(y = vv, x = perc, fill = wl)) +
  
  geom_col(width = 0.8, position = "dodge") +
  
  scale_x_continuous(limits = c(0, 100), breaks = c(0, 100), labels = c("Never", "Every\nyear")) +
  
  scale_fill_manual(values = c("#5b95cc", "#fd7a7c")) +
  
  theme_classic() +
  coord_cartesian(expand = F) +
  theme(#axis.ticks.y = element_blank(),
    #axis.text.y = element_blank(),
    axis.title.y = element_blank(),
    legend.title = element_blank(),
    legend.position = c(1.01, 0.9),
    plot.margin = unit(c(1,1.8,0.8,1), "cm"),
    text = element_text(size = 15),
    axis.text.x = element_text(vjust = -0.7)) +
  
  labs(x = "\ndays")




tb %>% 
  group_by(gcm,rcm, wl, yr = year(as_date(time))) %>% 
  summarize(v = sum(v > 28)) %>%
  # slice_sample(n = 21) %>%
  ungroup() %>% 
  mutate(wl = ifelse(wl == "0.5", "0.5 °C", "3.0 °C")) %>% 
  
  mutate(vv = case_when(v < 1 ~ "0",
                        v <= 7 ~ "1-7",
                        v <= 14 ~ "7-14",
                        v <= 30 ~ "14-30",
                        v <= 90 ~ "30-90",
                        TRUE ~ "90-180") %>% 
           factor(levels = rev(c("0",
                                 "1-7",
                                 "7-14",
                                 "14-30",
                                 "30-90",
                                 "90-180")))) %>% 
  
  group_by(wl, vv, .drop = F) %>% 
  summarize(n = n()) %>% 
  mutate(perc = n/sum(n)*100) %>% 
  
  ggplot(aes(y = vv, x = perc, fill = wl)) +
  
  geom_col(width = 0.8, position = "dodge") +
  
  scale_x_continuous(limits = c(0, 100), breaks = c(0, 100), labels = c("Never", "Every year")) +
  
  scale_fill_manual(values = c("#5b95cc", "#fd7a7c")) +
  
  theme_classic() +
  coord_cartesian(expand = F) +
  theme(
    axis.title.x = element_blank(),
    legend.title = element_blank(),
    legend.position = c(1.01, 0.9),
    plot.margin = unit(c(1,1.8,0.8,1), "cm"),
    text = element_text(size = 15),
    axis.text.x = element_text(vjust = -0.01)
  ) +
  labs(y = "days")
