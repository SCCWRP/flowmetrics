######
# random forest models of flow metrics

library(tidyverse)
library(lubridate)
library(sf)
library(randomForest)

data(flowmet)
data(comid_atts)

metsel <- 'x5_HighDur'

# separate models for each metric, month, flow year (wet, dry, avg)
# try with all streamcat variables, check importance, then try again
# use cross-validation

comid_atts <- comid_atts %>% 
  st_set_geometry(NULL)

# setup data to model
# includes folds, need to make sure these are the same folds across each metric
tomod <- flowmet %>% 
  mutate(
    mo = month(date),
    yr = year(date)
  ) %>% 
  group_by(mo) %>% 
  mutate(folds = sample(1:5, length(mo), replace = T)) %>% 
  ungroup %>% 
  gather('var', 'val', -watershedID, -COMID, -date, -mo, -yr) %>% 
  filter(var %in% metsel) %>% 
  left_join(comid_atts, by = 'COMID') %>% 
  group_by(mo) %>% 
  nest

# setup the models
mods <- tomod %>% 
  mutate(
    modall = map(data, funtion(x){
      
      browser()
      
    })
  )

