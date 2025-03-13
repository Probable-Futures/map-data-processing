
fun_list <- 
  
  list(
    
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
        mutate(tp = tp |> units::set_units(mm/d)) |> 
        st_apply(c(1,2), \(x){
          
          if (all(is.na(x))){
            
            rep(NA, yrs)
            
          } else {
            
            # running sum
            runsum <- 
              x %>%
              units::set_units(mm/d) |> 
              units::drop_units() |> 
              slider::slide_dbl(sum, 
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
              pr[i] <- max_pos #runsum[max_pos]
              
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
  
  
  












