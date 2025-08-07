

var_info_list <- 
  list(days_above_32c = list(units = "d",
                             input_vars = "tasmax",
                             name_era = "2m_maximum_temperature",
                             name_cordex = "maximum_temperature"),
       
       days_above_35c = list(units = "d",
                             input_vars = "tasmax",
                             name_era = "2m_maximum_temperature",
                             name_cordex = "maximum_temperature"),
       
       days_above_38c = list(units = "d",
                             input_vars = "tasmax",
                             name_era = "2m_maximum_temperature", 
                             name_cordex = "maximum_temperature"),
       
       days_above_45c = list(units = "d",
                             input_vars = "tasmax",
                             name_era = "2m_maximum_temperature",
                             name_cordex = "maximum_temperature"),
       
       average_temperature = list(units = "K",
                                  input_vars = "tas",
                                  name_era = "2m_temperature",
                                  name_cordex = "average_temperature"),
       
       average_daytime_temperature = list(units = "K",
                                          input_vars = "tasmax",
                                          name_era = "2m_maximum_temperature",
                                          name_cordex = "maximum_temperature"),
       
       ten_hottest_days = list(units = "K",
                               input_vars = "tasmax",
                               name_era = "2m_maximum_temperature",
                               name_cordex = "maximum_temperature"),
       
       freezing_days = list(units = "d",
                            input_vars = "tasmax",
                            name_era = "2m_maximum_temperature",
                            name_cordex = "maximum_temperature"),
       
       frost_nights = list(units = "d",
                           input_vars = "tasmin",
                           name_era = "2m_minimum_temperature",
                           name_cordex = "minimum_temperature"),
       
       nights_above_20c = list(units = "d",
                               input_vars = "tasmin",
                               name_era = "2m_minimum_temperature",
                               name_cordex = "minimum_temperature"),
       
       nights_above_25c = list(units = "d",
                               input_vars = "tasmin",
                               name_era = "2m_minimum_temperature",
                               name_cordex = "minimum_temperature"),
       
       average_nighttime_temperature = list(units = "K",
                                            input_vars = "tasmin",
                                            name_era = "2m_minimum_temperature",
                                            name_cordex = "minimum_temperature"),
       
       average_winter_temperature = list(units = "K",
                                         input_vars = "tas",
                                         name_era = "2m_temperature",
                                         name_cordex = "average_temperature"),
       
       ten_hottest_nights = list(units = "K",
                                 input_vars = "tasmin",
                                 name_era = "2m_minimum_temperature",
                                 name_cordex = "minimum_temperature"),
       
       days_above_26c_wb = list(units = "degC",
                                input_vars = "wb",
                                name_era = "wetbulb_temperature",
                                name_cordex = "wetbulb_temperature"),
       
       days_above_28c_wb = list(units = "degC",
                                input_vars = "wb",
                                name_era = "wetbulb_temperature",
                                name_cordex = "wetbulb_temperature"),
       
       days_above_30c_wb = list(units = "degC",
                                input_vars = "wb",
                                name_era = "wetbulb_temperature",
                                name_cordex = "wetbulb_temperature"),
       
       days_above_32c_wb = list(units = "degC",
                                input_vars = "wb",
                                name_era = "wetbulb_temperature",
                                name_cordex = "wetbulb_temperature"),
       
       ten_hottest_wb_days = list(units = "d",
                                  input_vars = "wb",
                                  name_era = "wetbulb_temperature",
                                  name_cordex = "wetbulb_temperature"),
       
       total_annual_precipitation = list(units = "mm",
                                         input_vars = "precip",
                                         name_era = "total_precipitation",
                                         name_cordex = "precipitation"),
       
       wettest_90_days = list(units = "mm",
                              input_vars = "precip",
                              name_era = "total_precipitation",
                              name_cordex = "precipitation"),
       
       wettest_day = list(units = "mm",
                          input_vars = "precip",
                          name_era = "total_precipitation",
                          name_cordex = "precipitation"),
       
       snowy_days = list(units = "d",
                         input_vars = c("precip", "tas"),
                         name_era = c("total_precipitation", "2m_temperature"),
                         name_cordex = c("precipitation", "average_temperature")),
       
       dry_hot_days = list(units = "d",
                           input_vars = c("precip", "tasmax"),
                           name_era = c("total_precipitation", "2m_maximum_temperature"),
                           name_cordex = c("precipitation", "maximum_temperature"))
)