######
# random forest models of flow metrics

library(tidyverse)
library(sf)
library(randomForest)

data(flowmet)
data(comid_atts)

tomod <- 'X5_HighDur'

# separate models for each metric, month, flow year (wet, dry, avg)
# try with all streamcat variables, check importance, then try again
# use cross-validation