######
# pre-processing data for prediction of streamflow metrics

library(tidyverse)
library(lubridate)
library(foreach)
library(doParallel)
library(rnoaa)
library(sf)
library(randomForest)

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

##
# get ave/wet/dry
data(dayprcp)

# cumulative precipitation by calendar year
# dry, ave, wet based on equal quantiles
yrprcp <- dayprcp %>% 
  mutate(
    yr = year(date) 
  ) %>% 
  group_by(yr) %>% 
  summarize(
    totprcp = sum(prcpmm, na.rm = T)
  ) %>% 
  ungroup %>% 
  mutate(
    catprcp = cut(totprcp,
                  breaks = c(-Inf, quantile(totprcp, c(0.33, 0.66)), Inf), 
                  labels = c('dry', 'ave', 'wet')
                  )
  )

save(yrprcp, file = 'data/yrprcp.RData', compress = 'xz')

######
# random forest models of flow metrics, performance

data(flowmet)
data(yrprcp)
data(comid_atts)

# remote geometry from comid
comid_atts <- comid_atts %>% 
  st_set_geometry(NULL)

# remove totprcp column
yrprcp <- yrprcp %>% 
  select(-totprcp)

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
  gather('var', 'val', -watershedID, -COMID, -date, -mo, -yr, -folds) %>% 
  left_join(comid_atts, by = 'COMID') %>% 
  left_join(yrprcp, by = 'yr') %>% 
  group_by(var, mo, catprcp) %>% 
  nest

# setup parallel
ncores <- detectCores() - 1 
cl<-makeCluster(ncores)
registerDoParallel(cl)
strt<-Sys.time()

# create models to eval
modsprf <- foreach(rw = 1:nrow(tomod), .packages = c('randomForest', 'tidyverse')) %dopar% {

  # log
  sink('log.txt')
  cat(rw, 'of', nrow(tomod), '\n')
  print(Sys.time()-strt)
  sink()
  
  # get index selections
  var <- tomod[rw, 'var']
  mo <- tomod[rw, 'mo']
  catprcp <- as.character(tomod[rw, 'catprcp'])
  data <- tomod$data[[rw]]
  
  # model formula, all
  frm <- names(data)[!names(data) %in% c('watershedID', 'COMID', 'date', 'yr', 'folds', 'var', 'val')] %>% 
    paste(collapse = ' + ') %>% 
    paste0('val ~ ', .) %>% 
    formula
  
  # folds
  flds <- unique(data$folds)
  
  # pre-allocated output
  out <- vector('list', length = length(flds))
  
  # loop through folds
  for(fld in flds){
    
    # calibration data
    calset <- data %>% 
      filter(!folds %in% fld)
    
    # validation data
    valset <- data %>% 
      filter(folds %in% fld)
    
    # create model
    mod <- randomForest(frm, data = calset, ntree = 1000, importance = TRUE, na.action = na.omit, keep.inbag = TRUE)
    
    # top ten important variables
    impvars <- mod$importance %>% 
      as.data.frame %>% 
      tibble::rownames_to_column('var') %>% 
      arrange(-`%IncMSE`) %>% 
      pull(var) %>% 
      .[1:10]
    
    # new formula, from top ten
    frmimp <- impvars %>% 
      paste(collapse = '+') %>% 
      paste0('val~', .) %>% 
      formula
    
    # create model, from top ten
    modimp <- randomForest(frmimp, data = calset, ntree = 1000, importance = TRUE, na.action = na.omit, keep.inbag = TRUE)
    
    # oob predictions for mod, modimp
    calsetid <- calset %>% 
      mutate(id = 1:nrow(.)) %>% 
      select(id)
    prd <- predict(mod) %>% 
      data.frame(prd = .) %>% 
      rownames_to_column('id') %>% 
      mutate(id = as.numeric(id)) %>% 
      left_join(calsetid, ., by = 'id') %>% 
      pull(prd)
    prdimp <- predict(modimp) %>% 
      data.frame(prd = .) %>% 
      rownames_to_column('id') %>% 
      mutate(id = as.numeric(id)) %>% 
      left_join(calsetid, ., by = 'id') %>% 
      pull(prd)
    
    # calibration prediction, full and important
    calpred <- tibble(
      set = 'cal',
      COMID = calset$COMID, 
      date= calset$date,
      obs = calset$val, 
      prd = prd,
      prdimp = prdimp
    )
    
    # validation prediction, full and important
    valpred <- tibble(
      set = 'val', 
      COMID = valset$COMID,
      date = valset$date,
      obs = valset$val, 
      prd = predict(mod, newdata = valset),
      prdimp = predict(modimp, newdata = valset)
    )
    
    # combine cal, val, get summary stats
    prds <- bind_rows(calpred, valpred) %>% 
      gather('modtyp', 'modprd', prd, prdimp) %>% 
      group_by(set, modtyp) %>% 
      nest() %>% 
      mutate(
        rmse = map(data, function(x) sqrt(mean(x$obs - x$modprd, na.rm = T)^2)), 
        rsqr = map(data, function(x){
          lm(obs ~ modprd, data = x) %>% 
            summary %>%
            .$r.squared
        }), 
        impvars = list(impvars)
      ) %>% 
      unnest(rmse, rsqr)
    
    # append to output
    out[[fld]] <- prds
    
  }
  
  # combine all fold data
  out <- out %>% 
    enframe('fld') %>% 
    unnest
  
  return(out)
    
}

# final output
flowmetest <- tomod %>% 
  select(-data) %>% 
  bind_cols(., enframe(modsprf)) %>% 
  select(-name) %>% 
  unnest(value)

save(flowmetprf, file = 'data/flowmetprf.RData', compress = 'xz')

######
# random forest models of flow metrics, predicted

data(flowmet)
data(yrprcp)
data(comid_atts)

# remote geometry from comid
comid_atts_nogm <- comid_atts %>% 
  st_set_geometry(NULL)

# remove totprcp column
yrprcp <- yrprcp %>% 
  select(-totprcp)

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
  gather('var', 'val', -watershedID, -COMID, -date, -mo, -yr, -folds) %>% 
  left_join(comid_atts_nogm, by = 'COMID') %>% 
  left_join(yrprcp, by = 'yr') %>% 
  group_by(var, mo, catprcp) %>% 
  nest

# setup parallel
ncores <- detectCores() - 1 
cl<-makeCluster(ncores)
registerDoParallel(cl)
strt<-Sys.time()

# create models for metric predictions
modsest <- foreach(rw = 1:nrow(tomod), .packages = c('randomForest', 'tidyverse')) %dopar% {
  
  # log
  sink('log.txt')
  cat(rw, 'of', nrow(tomod), '\n')
  print(Sys.time()-strt)
  sink()
  
  # get index selections
  var <- tomod[rw, 'var']
  mo <- tomod[rw, 'mo']
  catprcp <- as.character(tomod[rw, 'catprcp'])
  data <- tomod$data[[rw]]
  
  # model formula, all
  frm <- names(data)[!names(data) %in% c('watershedID', 'COMID', 'date', 'yr', 'folds', 'var', 'val')] %>% 
    paste(collapse = ' + ') %>% 
    paste0('val ~ ', .) %>% 
    formula
  
  # use only one fold
  fld <- 1
  
  # calibration data
  calset <- data %>% 
    filter(!folds %in% fld)
  
  # create model
  mod <- randomForest(frm, data = calset, ntree = 1000, importance = TRUE, na.action = na.omit, keep.inbag = TRUE)
  
  # top ten important variables
  impvars <- mod$importance %>% 
    as.data.frame %>% 
    tibble::rownames_to_column('var') %>% 
    arrange(-`%IncMSE`) %>% 
    pull(var) %>% 
    .[1:10]
    
  # new formula, from top ten
  frmimp <- impvars %>% 
    paste(collapse = '+') %>% 
    paste0('val~', .) %>% 
    formula

  # create model, from top ten
  modimp <- randomForest(frmimp, data = calset, ntree = 1000, importance = TRUE, na.action = na.omit, keep.inbag = TRUE)
  
  # predictions, all comid attributes
  prdimp <- predict(modimp, newdata = comid_atts_nogm, predict.all = T)
  bnds <- apply(prdimp$individual, 1, function(x) quantile(x, probs = c(0.1, 0.9), na.rm = T))
  cv <- apply(prdimp$individual, 1, function(x) sd(x, na.rm = T)/mean(x, na.rm = T))

  # combine output, estimates, lo, hi, cv, dataset
  out <- tibble(
    COMID = comid_atts_nogm$COMID, 
    est = prdimp$aggregate,
    lov= bnds[1, ],
    hiv= bnds[2, ], 
    cv = coef
    ) %>% 
    mutate(set = ifelse(COMID %in% calset$COMID, 'cal', 'notcal'))
  
  return(out)
  
}

# final output
flowmetest <- tomod %>% 
  select(-data) %>% 
  bind_cols(., enframe(modsest)) %>% 
  select(-name) %>% 
  unnest(value)

# get comid geometry
comid_atts_gm <- comid_atts %>% 
  select(COMID)

# join geometry
flowmetest <- flowmetest %>% 
  left_join(comid_atts_gm, by = 'COMID') %>% 
  st_as_sf()

save(flowmetest, file = 'data/flowmetest.RData', compress = 'xz')
