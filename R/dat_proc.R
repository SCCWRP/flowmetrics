######
# pre-processing data for prediction of streamflow metrics

library(tidyverse)
library(lubridate)
library(foreach)
library(doParallel)
library(rnoaa)

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

######
# get coarse daily estimates of precip for la
# from rnoaa

# days to select
dts <- c('1985-01-01', '2014-12-31') %>% 
  as.Date
dts <- seq.Date(from = dts[1], to = dts[2], by = 'day')

# la lat, lon, not used
lalat <- 34.0522; lalon <- 360 - 118.2437 

# setup clustering
ncores <- detectCores() - 2  
cl<-makeCluster(ncores)
registerDoParallel(cl)
strt<-Sys.time()

# get gridded data, selected long/lat is approximate to la
precip <- foreach(dt = seq_along(dts), .packages = c('tidyverse', 'rnoaa')) %dopar% {
  
  # log
  sink('log.txt')
  cat(dt, 'of', length(dts), '\n')
  print(Sys.time()-strt)
  sink()
  
  tmp <- cpc_prcp(dts[dt], us = T) %>% 
    filter(lon == 241.875 & lat == 34.125)
  
  tmp$precip
  
}

# precipitation (mm) with dates
dayprcp <- data.frame(
  date = dts, 
  prcpmm = unlist(precip)
  ) %>% 
  mutate(
    prcpmm = ifelse(prcpmm == -99.9, NA, prcpmm)
  )

save(dayprcp, file = 'data/dayprcp.RData', compress = 'xz')