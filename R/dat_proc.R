######
# pre-processing data for prediction of streamflow metrics

library(tidyverse)
library(lubridate)

##
# flow metrics to predict

flowmet <- read.csv('raw/final_metrics.csv', stringsAsFactors = F) %>% 
  select(SITE.x, COMID, date, tenyr, twoyr, X5yrRBI, x3_SFR, all_R10D.5, x3_QmaxIDR, x5_RecessMaxLength, all_LowDur, x5_HighDur, x3_HighDur, x10_HighNum, all_MedianNoFlowDays, all_Qmax, all_Q99) %>% 
  rename(
    watershedID = SITE.x
  ) %>% 
  mutate(
    date = ymd(date)
  )

save(flowmet, file = 'data/flowmet.RData', compress = 'xz')
