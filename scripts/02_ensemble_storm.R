
source("scripts/setup.R")
source("scripts/functions.R")

plan(multicore)


dir_bucket_mine <- "/mnt/bucket_mine"


doms <- c("SEA", "AUS", "CAS", "WAS", "EAS", "AFR", "EUR", "NAM", "CAM", "SAM")

old_wl <- c("19712000",
            "1C",
            "15C",
            "2C",
            "25C",
            "3C")

tb_vs <- 
  tibble(new = 
           c("100yr-storm-precip",
             "100yr-storm-freq"),
         old = 
           c("pr_100yr_storm",
             "return_period_100yr_storm")
  )

pwalk(tb_vs, function(new, old){

  print(new)
  
  d <- "/mnt/bucket_cmip5/Probable_futures/water_module/rcm_data/{old}/annual_by_time_span" %>% 
    str_glue()
  
  walk(doms, function(dom){
    
    print(dom)
    
    
    if(str_detect(new, "freq")){
      old_wl <- old_wl[-1]
    }
    
    
    rr <- 
      
      map(old_wl, function(wl){
        
        print(wl)
        
        f <- 
          d %>% 
          list.files() %>% 
          str_subset(dom) %>% 
          str_subset(wl)
        
        if(dom %in% c("SAM", "AUS")){
          
          f <- f[str_detect(f, "remo")]
          
        }
        
        s <- 
          f %>% 
          map(~str_glue("{d}/{.x}") %>% read_ncdf(proxy = F)) %>% 
          suppressMessages() %>% 
          {do.call(c, c(., along = 3))}
        
        if(str_detect(new, "precip")){
          s <- drop_units(s)
        }
        
        
        r <- 
          s %>% 
          st_apply(c(1,2), function(x){
            
            if(any(is.na(x))){
              c(mean = NA,
                perc50 = NA)
              
            } else {
              
              c(mean = mean(x),
                perc50 = quantile(x, prob = 0.5) %>% unname())  
              
            }
            
          },
          FUTURE = T,
          .fname = "stats") %>% 
          split("stats")
        
        return(r)
        
      })
    
    
    wl_vals <- seq(0.5, 3, by = 0.5)
    
    if(str_detect(new, "freq")){
      wl_vals <- wl_vals[-1]
    }
    
    
    s_result <- 
      rr %>% 
      {do.call(c, c(., along = "warming_levels"))} %>% 
      st_set_dimensions(3, values = wl_vals)
    
    
    res_filename <- 
      str_glue(
        "{dir_bucket_mine}/results/global_heat_pf/02_ensembled/{dom}_{new}_ensemble.nc"
      )
    
    fn_save_nc(res_filename, s_result)
    
    
    
  })
  
})










# rr_change <- 
#   rr[2:6] %>% 
#   map(function(s){
#     
#     s - rr[[1]]
#     
#   }) %>% 
#   {append(list(rr[[1]]), .)}









walk(doms, function(dom){
  
  print(dom)
  
  d <- "/mnt/bucket_cmip5/Probable_futures/water_module/rcm_data/{tb_vs$old[2]}/annual_by_time_span" %>% 
    str_glue()
  
  old_wl <- old_wl[-1]
  
  rr <- 
    
    map(old_wl, function(wl){
      
      print(wl)
      
      f <- 
        d %>% 
        list.files() %>% 
        str_subset(dom) %>% 
        str_subset(wl)
      
      if(dom %in% c("SAM", "AUS")){
        
        f <- f[str_detect(f, "remo")]
        
      }
      
      s <- 
        f %>% 
        map(~str_glue("{d}/{.x}") %>% read_ncdf(proxy = F)) %>% 
        suppressMessages() %>% 
        {do.call(c, c(., along = 3))}
      
      
      r <- 
        s %>% 
        st_apply(c(1,2), function(x){
          
          if(any(is.na(x))){
            c(mean = NA,
              perc50 = NA)
            
          } else {
            
            c(mean = mean(x),
              perc50 = quantile(x, prob = 0.5) %>% unname())  
            
          }
          
        },
        FUTURE = T,
        .fname = "stats") %>% 
        split("stats")
      
      return(r)
      
    })
  
  rr_change <- 
    rr %>% 
    map(function(s){
      
      (1-s) / 0.01
      
    })
  
  s_result <- 
    rr_change %>% 
    {do.call(c, c(., along = "warming_levels"))} %>% 
    st_set_dimensions(3, values = seq(1.0, 3, by = 0.5))
  
  
  res_filename <- 
    str_glue(
      "{dir_bucket_mine}/results/global_heat_pf/02_ensembled/{dom}_{tb_vs$new[2]}_ensemble.nc"
    )
  
  fn_save_nc(res_filename, s_result)
  
})





