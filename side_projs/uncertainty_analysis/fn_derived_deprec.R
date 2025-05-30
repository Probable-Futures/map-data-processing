
fun_list <- 
  
  list(
    
    days_above_32c = function(s) {
      
      s$tasmax |> 
        mutate(days = if_else(t2m >= units::set_units(32, degC), 1L, 0L)) |> 
        select(days) |> 
        aggregate(sum, by = "1 year") |> 
        aperm(c(2,3,1))
      
    },
    
    
    # ****
    
    
    days_above_35c = function(s) {
      
      s$tasmax |> 
        mutate(days = if_else(t2m >= units::set_units(35, degC), 1L, 0L)) |> 
        select(days) |> 
        aggregate(sum, by = "1 year") |> 
        aperm(c(2,3,1))
      
    },
    
    
    # ****
    
    
    days_above_38c = function(s) {
      
      s$tasmax |> 
        mutate(days = if_else(t2m >= units::set_units(38, degC), 1L, 0L)) |> 
        select(days) |> 
        aggregate(sum, by = "1 year") |> 
        aperm(c(2,3,1))
      
    },
    
    
    # ****
    
    
    days_above_45c = function(s) {
      
      s$tasmax |> 
        mutate(days = if_else(t2m >= units::set_units(45, degC), 1L, 0L)) |> 
        select(days) |> 
        aggregate(sum, by = "1 year") |> 
        aperm(c(2,3,1))
      
    },
    
    
    # ****
    
    
    average_temperature = function(s) {
      
      s$tas |> 
        aggregate(mean, by = "1 year") |> 
        aperm(c(2,3,1))
      
    },
    
    
    # ****
      
      
    average_daytime_temperature = function(s) {
      
      s$tasmax |> 
        aggregate(mean, by = "1 year") |> 
        aperm(c(2,3,1))
      
    },
    
    
    # ****
    
    
    ten_hottest_days = function(s) {
      
      s$tasmax |> 
        aggregate(\(x) x |> sort() |> tail(10) |> mean(), 
                  by = "1 year") |> 
        aperm(c(2,3,1))
      
    },
    
    
    # ****
    
    
    freezing_days = function(s) {
      
      s$tasmax |> 
        mutate(days = if_else(t2m < units::set_units(0, degC), 1L, 0L)) |> 
        select(days) |> 
        aggregate(sum, by = "1 year") |> 
        aperm(c(2,3,1))
        
    },
    
    
    # ****
    
    
    frost_nights = function(s) {
      
      s$tasmin |> 
        mutate(days = if_else(t2m < units::set_units(0, degC), 1L, 0L)) |> 
        select(days) |> 
        aggregate(sum, by = "1 year") |> 
        aperm(c(2,3,1))
      
    },
    
    
    # ****
    
    
    nights_above_20c = function(s) {
      
      s$tasmin |> 
        mutate(days = if_else(t2m >= units::set_units(20, degC), 1L, 0L)) |> 
        select(days) |> 
        aggregate(sum, by = "1 year") |> 
        aperm(c(2,3,1))
      
    },
    
    
    # ****
    
    
    nights_above_25c = function(s) {
      
      s$tasmin |> 
        mutate(days = if_else(t2m >= units::set_units(25, degC), 1L, 0L)) |> 
        select(days) |> 
        aggregate(sum, by = "1 year") |> 
        aperm(c(2,3,1))
      
    },
    
    
    # ****
    
    
    average_nighttime_temperature = function(s) {
      
      s$tasmin |> 
        aggregate(mean, by = "1 year") |> 
        aperm(c(2,3,1))
      
    },
    
    
    # ****
    
    
    average_winter_temperature = function(s) {
      
      ss <- 
        c(s$tas,
          s$tas |> 
            st_dim_to_attr(2))
      
      s_north <-
        ss |> 
        filter(month(time) %in% c(12,1,2)) |> 
        mutate(n = if_else(latitude >= 0, t2m, NA)) |> 
        select(n) |> 
        aggregate(by = "1 year", mean) |> 
        aperm(c(2,3,1))
      
      s_south <-
        ss |> 
        filter(month(time) %in% c(6,7,8)) |> 
        mutate(s = if_else(latitude < 0, t2m, NA)) |> 
        select(s) |> 
        aggregate(by = "1 year", mean) |> 
        aperm(c(2,3,1))
      
      c(s_north, s_south, along = "hemi") |>
        st_apply(c(1,2,3), sum, na.rm = T, .fname = "t2m")
      
    },
    
    
    # ****
    
    
    ten_hottest_nights  = function(s) {
      
      s$tasmin |> 
        aggregate(\(x) x |> sort() |> tail(10) |> mean(), 
                  by = "1 year") |> 
        aperm(c(2,3,1))
      
    },
    
    
    # ****
    
    
    days_above_26c_wb = function(s) {
      
      s$wb |> 
        mutate(days = if_else(wb >= units::set_units(26, degC), 1L, 0L)) |> 
        select(days) |> 
        aggregate(sum, by = "1 year") |> 
        aperm(c(2,3,1))
      
    },
    
    
    # ****
    
    
    days_above_28c_wb = function(s) {
      
      s$wb |> 
        mutate(days = if_else(wb >= units::set_units(28, degC), 1L, 0L)) |> 
        select(days) |> 
        aggregate(sum, by = "1 year") |> 
        aperm(c(2,3,1))
      
    },
    
    
    # ****
    
    
    days_above_30c_wb = function(s) {
      
      s$wb |> 
        mutate(days = if_else(wb >= units::set_units(30, degC), 1L, 0L)) |> 
        select(days) |> 
        aggregate(sum, by = "1 year") |> 
        aperm(c(2,3,1))
      
    },
    
    
    # ****
    
    
    days_above_32c_wb = function(s) {
      
      s$wb |> 
        mutate(days = if_else(wb >= units::set_units(32, degC), 1L, 0L)) |> 
        select(days) |> 
        aggregate(sum, by = "1 year") |> 
        aperm(c(2,3,1))
      
    },
    
    
    # ****
    
    
    ten_hottest_wb_days  = function(s) {
      
      s$wb |> 
        aggregate(\(x) x |> sort() |> tail(10) |> mean(), 
                  by = "1 year") |> 
        aperm(c(2,3,1))
      
    },
    
    
    # ****
    
    
    total_annual_precipitation = function(s) {
        
        time_dim <- 
          s$precip |> 
          st_get_dimension_values(3) |> 
          as_date()
        
        s$precip |>
          mutate(tp = tp |> units::set_units(mm/d)) |> 
          st_apply(c(1,2), \(x){
            
            if (all(is.na(x))){
              
              rep(NA, length(yrs))
              
            } else {
              
              aggregate(x, by = list(year(time_dim)), FUN = sum)$x
              
            }
            
          },
          FUTURE = T,
          .fname = "time") |> 
          aperm(c(2,3,1)) |> 
          st_set_dimensions(3, values = seq(as_date(paste0(first(yrs),"-01-01")),
                                            as_date(paste0(last(yrs),"-01-01")),
                                            by = "1 year"))
        
      },
    
    
    # *****
    
    
    wettest_90_days = function(s) {
      
      time_dim <- 
        s$precip |> 
        st_get_dimension_values(3) |> 
        as_date()
      
      s$precip |>
        # mutate(tp = tp |> units::set_units(mm/d)) |> 
        st_apply(c(1,2), \(x){
          
          if (all(is.na(x))){
            
            rep(NA, length(yrs))                                                       # <- AGHHHHH
            
          } else {
            
            # running sum
            runsum <- 
              x %>%
              # units::set_units(mm/d) |> 
              # units::drop_units() |> 
              slider::slide_dbl(.f = sum, 
                                .before = 89, 
                                .complete = T, 
                                .step = 2)
            
            # initialize results vector
            pr <- rep(NA_real_, length(yrs))
            
            # initial previous max position
            prev_max_pos <- -90
            
            # loop through years
            for(i in seq_along(yrs)){
              
              year_positions <- which(year(time_dim) == yrs[i])
              
              # avoid window overlap:
              # shorten the valid range of dates if the previous max happened
              # less than 90 days before the change of year (valid start should 
              # be at least 90 days apart from prev max)
              valid_start <- max(first(year_positions), prev_max_pos + 90)
              valid_end <- last(year_positions)
              
              valid_range <- valid_start:valid_end
              max_pos <- valid_range[which.max(runsum[valid_range])]
              
              # update results vector
              pr[i] <- runsum[max_pos]                        # AGHHHHHHHHHHHHH!!!!!!
              
              # update previous max position
              prev_max_pos <- max_pos
              
            }
            
            pr
            
          }
          
        },
        FUTURE = T,
        .fname = "time") |>
        aperm(c(2,3,1)) |> 
        st_set_dimensions(3, values = seq(as_date(paste0(first(yrs),"-01-01")),
                                          as_date(paste0(last(yrs),"-01-01")),
                                          by = "1 year"))
      
    },
    
    
    # *****
    
    
    snowy_days = function(s) {
      
      c(s$precip, s$tas) |> 
        mutate(days = if_else(tp >= units::set_units(1, mm/d) & t2m < units::set_units(0, degC), 1L, 0L)) |> 
        select(days) |> 
        aggregate(sum, by = "1 year") |> 
        aperm(c(2,3,1))
      
    },
    
    
    # *****
    
    
    dry_hot_days = function(s) {
      
      time_dim <- 
        s$precip |> 
        st_get_dimension_values(3) |> 
        as_date()
      
      base_lims <- 
        c(first(which(year(time_dim) == 1971)),
          last(which(year(time_dim) == 2000)))
      
      c(units::drop_units(s$precip), 
        units::drop_units(s$tasmax), 
        along = "v") |> 
        
        st_apply(c(1,2), \(x){
          
          if (all(is.na(x[,1]))){
            
            rep(NA, length(yrs))
            
          } else {
            
            precip_cond <- x[,1] < quantile(x[,1][base_lims[1]:base_lims[2]], 0.1) 
            tasmax_cond <- x[,2] >= quantile(x[,2][base_lims[1]:base_lims[2]], 0.9)
            
            joint_cond <- precip_cond & tasmax_cond
            
            aggregate(joint_cond, by = list(year(time_dim)), sum)$x
            
          }
          
        },
        FUTURE = T,
        .fname = "time") |> 
        aperm(c(2,3,1)) |> 
        st_set_dimensions(3, values = seq(as_date(paste0(first(yrs),"-01-01")),
                                          as_date(paste0(last(yrs),"-01-01")),
                                          by = "1 year"))
      
    }
    
    
    
  )
  
  
  












