# front matter ------------------------------------------------------------

# pre-processing data for prediction of streamflow metrics

library(tidyverse)
library(lubridate)
library(foreach)
library(doParallel)
library(rnoaa)
library(sf)
library(randomForest)
library(h5)
library(raster)

source('R/funcs.R')

# basic data proc ---------------------------------------------------------

##
# all COMID streamcat attributes, sf polyline

# created in R/COMID clustering.R
# column names are the same as below, just expanded
data(comid_atts)

comid_attsall <- st_read('../COMID attributes.shp')
names(comid_attsall) <- names(comid_atts)
save(comid_attsall, file = 'data/comid_attsall.RData', compress = 'xz')

##
# observed bio data

biodat <- read.csv('raw/species_list_extrapolation.csv', stringsAsFactors = F) %>%
  dplyr::select(name, COMID, date) %>%
  mutate(
    date = ymd(date)
  ) %>%
  unique

save(biodat, file = 'data/biodat.RData', compress = 'xz')

##
# flow metrics to predict

flowmet <- read.csv('raw/final_metrics.csv', stringsAsFactors = F) %>%
  dplyr::select(-X, -SITE.y) %>%
  rename(
    watershedID = SITE.x
  ) %>%
  mutate(
    date = ymd(date)
  )

save(flowmet, file = 'data/flowmet.RData', compress = 'xz')

##
# COMID points as sf object, all COMIDs in study area including dammed/urban and no bio data

data(flowmet)

comid_pnts <- st_read('L:/Flow ecology and climate change_ES/Jenny/AirTemp/COMID_to_Point.shp') %>%
  dplyr::select(COMID) %>%
  st_zm()
save(comid_pnts, file = 'data/comid_pnts.RData', compress = 'xz')

# extract baseline precip from simulated data -----------------------------

data(comid_pnts)

# get recursive file list, daily for each water year
fls <- list.files('//172.16.1.198/SShare1/Forcing/PPT/Baseline', recursive = T, full.names = T)

# setup parallel backend
ncores <- detectCores() - 1
cl<-makeCluster(ncores)
registerDoParallel(cl)

bsext <- simextract_fun(fls, comid_pnts)

save(bsext, file = 'data/bsext.RData', compress = 'xz')

# get precipitation metrics at quarterly intervals  -----------------------
# this is done using extracted sim data, used for rf models below

##
# estimate Konrad metrics from extracted precip data above

data(bsext)
data(flowmet)

# select master ID
ID <- unique(flowmet$COMID) %>%
  sort

# setup parallel backend
cores <- detectCores() - 2
cl <- makeCluster(cores)
registerDoParallel(cl)
strt <- Sys.time()

# estimate konrad, ~2 hours
res <- foreach(i = c('x3', 'x5', 'x10', 'all'), .packages = c('tidyverse', 'lubridate')) %dopar% {

  konradfun(id = ID, flowin = bsext, subnm = i)

}
Sys.time() - strt

kradprecipmet <- do.call('rbind', res) %>%
  rename(COMID = stid)

##
# estimate additional metrics, not Konrad

data(bsext)
data(flowmet)

# select master ID
ID <- unique(flowmet$COMID) %>%
  sort

# setup parallel backend
cores <- detectCores() - 2
cl <- makeCluster(cores)
registerDoParallel(cl)
strt <- Sys.time()

#prep site file data, ~2 hrs
res <- foreach(i = c('x3', 'x5', 'x10', 'all'), .packages = c('tidyverse', 'lubridate')) %dopar% {


  out <- addlmet_fun(id = ID, flowin = bsext, subnm = i)
  return(out)

}
Sys.time() - strt

addlprecipmet <- do.call('rbind', res) %>%
  rename(COMID = stid)

##
# combine additional metrics and filter out extra stuff

precipmet <- rbind(kradprecipmet, addlprecipmet)

flomeans <- precipmet %>%
  filter(met %in% c('Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec')) %>%
  filter(ktype %in% c('all', 'x3'))  %>%
  dplyr::select(-ktype)

strms <- precipmet %>%
  filter(met %in% c('twoyr', 'fivyr', 'tenyr')) %>%
  filter(ktype %in% c('all', 'x3'))  %>%
  dplyr::select(-ktype)

rbi <- precipmet %>%
  filter(grepl('RBI|rbi', met)) %>%
  filter(ktype %in% c('all', 'x3')) %>%
  mutate(
    met = gsub('yr', '_', met),
    met = ifelse(grepl('\\_', met), paste0('x', met), met)
    ) %>%
  dplyr::select(-ktype)

# remove flo means, storm events, rbi because above used instead
# make complete cases to fill 'all' metrics to start dates
# remove R10 metrics from Konrad, didnt process
precipmet <- precipmet %>%
  filter(!met %in% c('Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec')) %>%
  filter(!met %in% c('twoyr', 'fivyr', 'tenyr')) %>%
  filter(!grepl('RBI|rbi', met)) %>%
  unite('met', ktype, met, sep = '_') %>%
  bind_rows(flomeans, strms, rbi) %>%
  complete(COMID, date, met) %>%
  group_by(COMID, met) %>%
  mutate(
    val = ifelse(grepl('^all', met), na.omit(val), val)
  ) %>%
  na.omit %>%
  spread(met, val) %>%
  ungroup %>%
  mutate(
    COMID = as.numeric(COMID)
  )

save(precipmet, file = 'data/precipmet.RData', compress = 'xz')

# get perf of rf models for every flow metric -----------------------------
# uses precip metrics above

data(flowmet)
data(comid_atts)
data(precipmet)

# remote geometry from comid
comid_atts <- comid_atts %>%
  st_set_geometry(NULL)

# setup data to model
# includes folds
tomod <- flowmet %>%
  # dplyr::select(-tenyr) %>% # do not model tenyr
  mutate(
    mo = month(date)
  ) %>%
  group_by(mo) %>%
  mutate(folds = sample(1:5, length(mo), replace = T)) %>%
  ungroup %>%
  gather('var', 'val', -watershedID, -COMID, -date, -mo, -folds) %>%
  left_join(comid_atts, by = 'COMID') %>%
  left_join(precipmet, by = c('COMID', 'date')) %>%
  group_by(var, mo) %>%
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
  data <- tomod$data[[rw]]

  # model formula, use all predictor if ten yr, if not tenyr use all but tenyr
  frm <- names(data)[!names(data) %in% c('watershedID', 'COMID', 'date', 'yr', 'folds', 'var', 'val')]

  # if not tenyr, use all but tenyr
  if(var != 'tenyr')
    frm <- frm[!frm %in% 'tenyr']

  frm <- frm %>%
    paste(collapse = ' + ') %>%
    paste0('val ~ ', .) %>%
    formula

  # # folds
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
    set.seed(123)
    mod <- randomForest(frm, data = calset, ntree = 500, importance = TRUE, na.action = na.omit, keep.inbag = TRUE)

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
    set.seed(123)
    modimp <- randomForest(frmimp, data = calset, ntree = 500, importance = TRUE, na.action = na.omit, keep.inbag = TRUE)

    # oob predictions for mod, modimp
    calsetid <- calset %>%
      mutate(id = 1:nrow(.)) %>%
      dplyr::select(id)
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
flowmetprf <- tomod %>%
  dplyr::select(-data) %>%
  bind_cols(., enframe(modsprf)) %>%
  dplyr::select(-name) %>%
  unnest(value)

save(flowmetprf, file = 'data/flowmetprf.RData', compress = 'xz')

# pull precip metrics where we have bio data ------------------------------
# uses bsext as baseline precip
# uses biodata to get COMID and dates for pullling precip data
# biodata limited to max date in bsext and min date in bsext + 3 years (latter is only two records)
# precip metrics are used below to reconsruct rf models and get flow metric predictions

data(comid_pnts)
data(biodat)
data(bsext)

# comids where bio was observed, find nearest july date, this is a lookup table
# max date does not exceed that from simulated baseline precip data
biodatcomid <- biodat %>%
  dplyr::select(COMID, date) %>%
  filter(date <= max(bsext$date)) %>%
  unique %>%
  mutate(
    dtup = ymd(paste0(year(date) - 1, '-07-01')),
    dtdn = ymd(paste0(year(date) + 1, '-07-01')),
    dtyr = ymd(paste0(year(date), '-07-01'))
  ) %>%
  group_by(COMID, date) %>%
  nest %>%
  mutate(
    dtsl = purrr::pmap(list(data, date), function(data, date){

      date <- as.Date(date, origin = c('1970-01-01'))
      chkdf <- which.min(c(abs(data$dtup - date), abs(data$dtdn - date), abs(data$dtyr - date)))
      out <- data[1, chkdf, drop = T]

      return(out)

    })
  ) %>%
  unnest %>%
  dplyr::select(COMID, date, dtsl)

# from biodatcomid, get COMID and dtsl
# take unique, otherwise duplicated dates maybe calculated for metrics from dtsl
# select master ID
toproc <- biodatcomid %>%
  dplyr::select(COMID, dtsl) %>%
  unique
ID <- toproc$COMID
dtsls <- toproc$dtsl

# setup parallel backend
cores <- detectCores() - 2
cl <- makeCluster(cores)
registerDoParallel(cl)
strt <- Sys.time()

# estimate konrad, ~2 hours
res <- foreach(i = c('x3', 'x5', 'x10', 'all'), .packages = c('tidyverse', 'lubridate')) %dopar% {

  if(i == 'all')
    ID <- unique(ID)

  out <- konradfun(id = ID, flowin = bsext, dtend = dtsls, subnm = i)
  return(out)

}
Sys.time() - strt

kradprecipmet <- do.call('rbind', res) %>%
  rename(COMID = stid)

##
# estimate additional metrics, not Konrad

# setup parallel backend
cores <- detectCores() - 2
cl <- makeCluster(cores)
registerDoParallel(cl)
strt <- Sys.time()

#prep site file data, ~2 hrs
res <- foreach(i = c('x3', 'x5', 'x10', 'all'), .packages = c('tidyverse', 'lubridate')) %dopar% {

  if(i == 'all')
    ID <- unique(ID)

  out <- addlmet_fun(id = ID, flowin = bsext, dtend = dtsls, subnm = i)
  return(out)

}
Sys.time() - strt

addlprecipmet <- do.call('rbind', res) %>%
  rename(COMID = stid)

##
# combine additional metrics and filter out extra stuff

precipmet <- rbind(kradprecipmet, addlprecipmet)

flomeans <- precipmet %>%
  filter(met %in% c('Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec')) %>%
  filter(ktype %in% c('all', 'x3'))  %>%
  dplyr::select(-ktype)

strms <- precipmet %>%
  filter(met %in% c('twoyr', 'fivyr', 'tenyr')) %>%
  filter(ktype %in% c('all', 'x3'))  %>%
  dplyr::select(-ktype)

rbi <- precipmet %>%
  filter(grepl('RBI|rbi', met)) %>%
  filter(ktype %in% c('all', 'x3')) %>%
  mutate(
    met = gsub('yr', '_', met),
    met = ifelse(grepl('\\_', met), paste0('x', met), met)
  ) %>%
  dplyr::select(-ktype)

# remove flo means, storm events, rbi because above used instead
# make complete cases to fill 'all' metrics to start dates (must do sperately for COMID because of different dates)
# remove R10 metrics from Konrad, didnt process
precipmet <- precipmet %>%
  filter(!met %in% c('Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec')) %>%
  filter(!met %in% c('twoyr', 'fivyr', 'tenyr')) %>%
  filter(!grepl('RBI|rbi', met)) %>%
  unite('met', ktype, met, sep = '_') %>%
  bind_rows(flomeans, strms, rbi) %>%
  group_by(COMID) %>%
  nest %>%
  mutate(
    data = purrr::map(data, function(x){

      out <- x %>%
        complete(date, met) %>%
        group_by(met) %>%
        mutate(
          val = ifelse(grepl('^all', met), na.omit(val), val)
        ) %>%
        filter(!date %in% as.Date('2014-09-30')) %>%  # placeholder date from all metrics
        na.omit %>%
        # unique %>%
        spread(met, val)

      return(out)

    })
  ) %>%
  mutate(
    COMID = as.numeric(COMID)
  ) %>%
  ungroup %>%
  unnest %>%
  rename(
    dtsl = date
  )

# join to biodatcomid by dtsl
bioprecipmet <- biodatcomid %>%
  left_join(precipmet, by = c('COMID', 'dtsl'))

save(bioprecipmet, file = 'data/bioprecipmet.RData', compress = 'xz')

# predict baseline flowmetrics where observed bio ----------------------------------
# uses extracted precip metrics, static streamcat predictors
# best predictors for rf models above

data(flowmet)
data(precipmet)
data(flowmetprf)
data(bioprecipmet)
data(comid_attsall)
data(biodat)

# setup parallel
ncores <- detectCores() - 2
cl<-makeCluster(ncores)
registerDoParallel(cl)

bioflowmetest <- flowmetprd_fun(flowmet, precipmet, flowmetprf, bioprecipmet, comid_attsall)

# join with biology (only date, COMID were modelled, species doesn't matter)
bioflowmetest <- bioflowmetest %>%
  left_join(biodat, ., by = c('COMID', 'date')) %>%
  dplyr::select(name, COMID, date, var, est) %>%
  spread(var, est)

save(bioflowmetest, file = 'data/bioflowmetest.RData', compress = 'xz')

# pull precip metrics for recent bio data --------------------------------------
# extract all precip metrics for locations with bio data that occurred after baseline precip sims

data(biodat)
data(bsext)

# biodata that occurred after baseline precip sims
# have to add dtsl as most recent date for which we have an rf model for the flow metric (by quarter)
recentdat <- biodat %>% 
  filter(date > as.Date('2014-09-30')) %>% 
  dplyr::select(COMID, date) %>%
  mutate(dtsl = as.Date('2014-07-01'))

# setup inputs
toproc <- recentdat %>% 
  dplyr::select(COMID, dtsl) %>% 
  unique

ID <- unique(toproc$COMID)
dtsls <- toproc$dtsl

# setup parallel backend
cores <- detectCores() - 2
cl <- makeCluster(cores)
registerDoParallel(cl)
strt <- Sys.time()

# estimate konrad, ~2 hours
res <- foreach(i = c('x3', 'x5', 'x10', 'all'), .packages = c('tidyverse', 'lubridate')) %dopar% {
  
  if(i == 'all')
    ID <- unique(ID)
  
  out <- konradfun(id = ID, flowin = bsext, dtend = dtsls, subnm = i)
  return(out)
  
}
Sys.time() - strt

kradprecipmet <- do.call('rbind', res) %>%
  rename(COMID = stid)

##
# estimate additional metrics, not Konrad

# setup parallel backend
cores <- detectCores() - 2
cl <- makeCluster(cores)
registerDoParallel(cl)
strt <- Sys.time()

#prep site file data, ~2 hrs
res <- foreach(i = c('x3', 'x5', 'x10', 'all'), .packages = c('tidyverse', 'lubridate')) %dopar% {
  
  if(i == 'all')
    ID <- unique(ID)
  
  out <- addlmet_fun(id = ID, flowin = bsext, dtend = dtsls, subnm = i)
  return(out)
  
}
Sys.time() - strt

addlprecipmet <- do.call('rbind', res) %>%
  rename(COMID = stid)

##
# combine additional metrics and filter out extra stuff

recentbioprecipmet <- rbind(kradprecipmet, addlprecipmet) #%>% 
# mutate(date = as.Date('2014-07-01')) 

flomeans <- recentbioprecipmet %>%
  filter(met %in% c('Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec')) %>%
  filter(ktype %in% c('all', 'x3'))  %>%
  dplyr::select(-ktype)

strms <- recentbioprecipmet %>%
  filter(met %in% c('twoyr', 'fivyr', 'tenyr')) %>%
  filter(ktype %in% c('all', 'x3'))  %>%
  dplyr::select(-ktype)

rbi <- recentbioprecipmet %>%
  filter(grepl('RBI|rbi', met)) %>%
  filter(ktype %in% c('all', 'x3')) %>%
  mutate(
    met = gsub('yr', '_', met),
    met = ifelse(grepl('\\_', met), paste0('x', met), met)
  ) %>%
  dplyr::select(-ktype)

# remove flo means, storm events, rbi because above used instead
# make complete cases to fill 'all' metrics to start dates (must do sperately for COMID because of different dates)
# remove R10 metrics from Konrad, didnt process
recentbioprecipmet <- recentbioprecipmet %>%
  filter(!met %in% c('Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec')) %>%
  filter(!met %in% c('twoyr', 'fivyr', 'tenyr')) %>%
  filter(!grepl('RBI|rbi', met)) %>%
  unite('met', ktype, met, sep = '_') %>%
  bind_rows(flomeans, strms, rbi) %>%
  group_by(COMID) %>%
  nest %>%
  mutate(
    data = purrr::map(data, function(x){
      
      out <- x %>%
        complete(date, met) %>%
        group_by(met) %>%
        mutate(
          val = ifelse(grepl('^all', met), na.omit(val), val)
        ) %>%
        filter(!date %in% as.Date('2014-09-30')) %>%  # placeholder date from all metrics
        na.omit %>%
        # unique %>%
        spread(met, val)
      
      return(out)
      
    })
  ) %>%
  mutate(
    COMID = as.numeric(COMID)
  ) %>%
  ungroup %>%
  unnest %>%
  rename(
    dtsl = date
  )

# join to biodatcomid by dtsl
recentbioprecipmet <- recentdat %>%
  left_join(recentbioprecipmet, by = c('COMID', 'dtsl'))

# predict baseline flowmetrics where observed recent bio ------------------

data(flowmet)
data(precipmet)
data(flowmetprf)
data(comid_attsall)
data(biodat)
data(bioflowmetest)

# setup parallel
ncores <- detectCores() - 2
cl<-makeCluster(ncores)
registerDoParallel(cl)

recentbioflowmetest <- flowmetprd_fun(flowmet, precipmet, flowmetprf, recentbioprecipmet, comid_attsall)

# biodata that occurred after baseline precip sims
recentdat <- biodat %>% 
  filter(date > as.Date('2014-09-30')) %>% 
  dplyr::select(name, COMID, date) %>%
  mutate(dtsl = as.Date('2014-07-01'))

# join with recent biology (only date, COMID were modelled, species doesn't matter)
recentbioflowmetest <- recentbioflowmetest %>%
  left_join(recentdat, ., by = c('COMID', 'date')) %>%
  dplyr::select(name, COMID, date, var, est) %>%
  filter(!grepl('^x10|^x5|^x3|^X10|^X5|^X3', var)) %>% 
  unique 

# join with existing bioflowmetest to add recent dates
bioflowmetest <- bioflowmetest %>% 
  gather('var', 'est', -name, -COMID, -date) %>% 
  left_join(recentbioflowmetest,by = c('name', 'COMID', 'date', 'var')) %>% 
  mutate(
    est.x = ifelse(is.na(est.x), est.y, est.x)
  ) %>% 
  dplyr::select(-est.y) %>% 
  spread(var, est.x)

save(bioflowmetest, file = 'data/bioflowmetest.RData', compress = 'xz')

# get baseline precip metrics, all comid ------------------------------
# uses bsext as baseline precip
# dates are quarterly within water year for selectd years
# biodata limited to max date in bsext and min date in bsext + 3 years (latter is only two records)
# precip metrics are used below to reconsruct rf models and get flow metric predictions

data(comid_pnts)
data(biodat)
data(flowmetprf)
data(bsext)

# 1993 - wet
# 2010 - moderate
# 2014 - dry

# dates for precip metric extraction
# dtsl <- c(1992, 1993, 1993, 1993, 2009, 2010, 2010, 2010, 2013, 2014, 2014, 2014) %>%
#   paste(., rep(c(9, 1, 4, 7), 3), 1, sep = '-') %>%
#   ymd
dtsl <- c(1993, 2010, 2014) %>%
  paste(., rep(c(7), 3), 1, sep = '-') %>%
  ymd

# combine dtsl with comid (all)
toproc <- crossing(
  COMID = comid_pnts$COMID,
  dtsl = dtsl
)

# passed to metric calculators
ID <- toproc$COMID
dtsls <- toproc$dtsl

# setup parallel backend
cores <- detectCores() - 2
cl <- makeCluster(cores)
registerDoParallel(cl)
strt <- Sys.time()

# estimate konrad, ~2 hours
res <- foreach(i = c('x3', 'x5', 'x10', 'all'), .packages = c('tidyverse', 'lubridate')) %dopar% {

  if(i == 'all')
    ID <- unique(ID)

  out <- konradfun(id = ID, flowin = bsext, dtend = dtsls, subnm = i)
  return(out)

}
Sys.time() - strt

kradprecipmet <- do.call('rbind', res) %>%
  rename(COMID = stid)

##
# estimate additional metrics, not Konrad

# setup parallel backend
cores <- detectCores() - 2
cl <- makeCluster(cores)
registerDoParallel(cl)
strt <- Sys.time()

#prep site file data, ~2 hrs
res <- foreach(i = c('x3', 'x5', 'x10', 'all'), .packages = c('tidyverse', 'lubridate')) %dopar% {

  if(i == 'all')
    ID <- unique(ID)

  out <- addlmet_fun(id = ID, flowin = bsext, dtend = dtsls, subnm = i)
  return(out)

}
Sys.time() - strt

addlprecipmet <- do.call('rbind', res) %>%
  rename(COMID = stid)

##
# combine additional metrics and filter out extra stuff

precipmet <- rbind(kradprecipmet, addlprecipmet)

flomeans <- precipmet %>%
  filter(met %in% c('Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec')) %>%
  filter(ktype %in% c('all', 'x3'))  %>%
  dplyr::select(-ktype)

strms <- precipmet %>%
  filter(met %in% c('twoyr', 'fivyr', 'tenyr')) %>%
  filter(ktype %in% c('all', 'x3'))  %>%
  dplyr::select(-ktype)

rbi <- precipmet %>%
  filter(grepl('RBI|rbi', met)) %>%
  filter(ktype %in% c('all', 'x3')) %>%
  mutate(
    met = gsub('yr', '_', met),
    met = ifelse(grepl('\\_', met), paste0('x', met), met)
  ) %>%
  dplyr::select(-ktype)

# remove flo means, storm events, rbi because above used instead
# make complete cases to fill 'all' metrics to start dates (must do sperately for COMID because of different dates)
# remove R10 metrics from Konrad, didnt process
precipmet <- precipmet %>%
  filter(!met %in% c('Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec')) %>%
  filter(!met %in% c('twoyr', 'fivyr', 'tenyr')) %>%
  filter(!grepl('RBI|rbi', met)) %>%
  unite('met', ktype, met, sep = '_') %>%
  bind_rows(flomeans, strms, rbi) %>%
  group_by(COMID) %>%
  nest %>%
  mutate(
    data = purrr::map(data, function(x){

      out <- x %>%
        complete(date, met) %>%
        group_by(met) %>%
        mutate(
          val = ifelse(grepl('^all', met), na.omit(val), val)
        ) %>%
        filter(!date %in% as.Date('2014-09-30')) %>%  # placeholder date from all metrics
        na.omit %>%
        # unique %>%
        spread(met, val)

      return(out)

    })
  ) %>%
  mutate(
    COMID = as.numeric(COMID)
  ) %>%
  ungroup %>%
  unnest %>%
  rename(
    dtsl = date
  )

# join to proc info by dtsl, COMID
bsprecipmet <- toproc %>%
  left_join(precipmet, by = c('COMID', 'dtsl'))

save(bsprecipmet, file = 'data/bsprecipmet.RData', compress = 'xz')

# predict baseline flowmetrics for selected years, all COMID --------------
# uses extracted precip metrics from all comid, static streamcat predictors
# best predictors for rf models above

data(flowmet)
data(precipmet)
data(flowmetprf)
data(bsprecipmet)
data(comid_attsall)

# setup parallel
ncores <- detectCores() - 2
cl<-makeCluster(ncores)
registerDoParallel(cl)

bsflowmetest <- flowmetprd_fun(flowmet, precipmet, flowmetprf, bsprecipmet, comid_attsall)

save(bsflowmetest, file = 'data/bsflowmetest.RData', compress = 'xz')


# extract future precip predictions from simulated data -------------------

data(comid_pnts)

##
# CanESM2 extraction

# get recursive file list, daily for 2200 - 2040, 2082 - 2100, book ends on july 1st
fls <- list.files('//172.16.1.198/SShare1/Forcing/PPT/CanESM2/rcp85/', recursive = T, full.names = T)
inds <- grep('\\_20220701|\\_20400701|\\_20820701|\\_21000701', fls)
fls <- fls[c(inds[1]:inds[2], inds[3]:inds[4])]

# setup parallel backend
ncores <- detectCores() - 1
cl<-makeCluster(ncores)
registerDoParallel(cl)

CanESM2ext <- simextract_fun(fls, comid_pnts)

save(CanESM2ext, file = 'data/CanESM2ext.RData', compress = 'xz')

##
# CCSM4 extraction

# get recursive file list, daily for 2200 - 2040, 2082 - 2100, book ends on july 1st
fls <- list.files('//172.16.1.198/SShare1/Forcing/PPT/CCSM4/rcp85/', recursive = T, full.names = T)
inds <- grep('\\_20220701|\\_20400701|\\_20820701|\\_21000701', fls)
fls <- fls[c(inds[1]:inds[2], inds[3]:inds[4])]

# setup parallel backend
ncores <- detectCores() - 1
cl<-makeCluster(ncores)
registerDoParallel(cl)

CCSM4ext <- simextract_fun(fls, comid_pnts)

save(CCSM4ext, file = 'data/CCSM4ext.RData', compress = 'xz')

##
# MIROC5 extraction

# get recursive file list, daily for 2200 - 2040, 2082 - 2100, book ends on july 1st
fls <- list.files('//172.16.1.198/SShare1/Forcing/PPT/MIROC5/rcp85/', recursive = T, full.names = T)
inds <- grep('\\_20220701|\\_20400701|\\_20820701|\\_21000701', fls)
fls <- fls[c(inds[1]:inds[2], inds[3]:inds[4])]

# setup parallel backend
ncores <- detectCores() - 1
cl<-makeCluster(ncores)
registerDoParallel(cl)

MIROC5ext <- simextract_fun(fls, comid_pnts)

save(MIROC5ext, file = 'data/MIROC5ext.RData', compress = 'xz')

# canesm2dt1 get precip mets ----------------------------------------------------------

data(comid_pnts)
data(CanESM2ext)

# input
toproc <- crossing(
  dtsl = as.Date(c('2040-07-01')),
  COMID = comid_pnts$COMID
)
ID <- toproc$COMID
dtsls <- toproc$dtsl

# subset precip daily time series by dates
CanESM2ext_sub <- CanESM2ext %>%
  filter(date <= max(dtsls))

##
# konrad

# setup parallel backend
cores <- detectCores() - 1
cl <- makeCluster(cores)
registerDoParallel(cl)

# log
strt <- Sys.time()

# estimate konrad
res <- foreach(i = c('x3', 'x5', 'x10', 'all'), .packages = c('tidyverse', 'lubridate')) %dopar% {

  if(i == 'all')
    ID <- unique(ID)

  out <- konradfun(id = ID, flowin = CanESM2ext_sub, dtend = dtsls, subnm = i)
  return(out)

}

print(Sys.time() - strt)

kradprecipmet <- do.call('rbind', res) %>%
  rename(COMID = stid)

##
# addl precip metrics

# setup parallel backend
cores <- detectCores() - 1
cl <- makeCluster(cores)
registerDoParallel(cl)

# log
strt <- Sys.time()

#prep site file data, ~2 hrs
res <- foreach(i = c('x3', 'x5', 'x10', 'all'), .packages = c('tidyverse', 'lubridate')) %dopar% {

  if(i == 'all')
    ID <- unique(ID)

  out <- addlmet_fun(id = ID, flowin = CanESM2ext_sub, dtend = dtsls, subnm = i)
  return(out)

}

print(Sys.time() - strt)

addlprecipmet <- do.call('rbind', res) %>%
  rename(COMID = stid)

canesm2precdt1 <- precipcmb_fun(kradprecipmet, addlprecipmet)

save(canesm2precdt1, file = 'data/canesm2precdt1.RData', compress = 'xz')

# canesm2dt2 get precip mets -------------------------------------------------------------

data(comid_pnts)
data(CanESM2ext)

# input
toproc <- crossing(
  dtsl = as.Date(c('2100-07-01')),
  COMID = comid_pnts$COMID
)
ID <- toproc$COMID
dtsls <- toproc$dtsl

# subset precip daily time series by dates
CanESM2ext_sub <- CanESM2ext %>%
  filter(date <= as.Date('2100-07-01') & date >= as.Date('2082-07-01'))

##
# konrad

# setup parallel backend
cores <- detectCores() - 1
cl <- makeCluster(cores)
registerDoParallel(cl)

# log
strt <- Sys.time()

# estimate konrad
res <- foreach(i = c('x3', 'x5', 'x10', 'all'), .packages = c('tidyverse', 'lubridate')) %dopar% {

  if(i == 'all')
    ID <- unique(ID)

  out <- konradfun(id = ID, flowin = CanESM2ext_sub, dtend = dtsls, subnm = i)
  return(out)

}

print(Sys.time() - strt)

kradprecipmet <- do.call('rbind', res) %>%
  rename(COMID = stid)

##
# addl precip metrics

# setup parallel backend
cores <- detectCores() - 1
cl <- makeCluster(cores)
registerDoParallel(cl)

# log
strt <- Sys.time()

#prep site file data, ~2 hrs
res <- foreach(i = c('x3', 'x5', 'x10', 'all'), .packages = c('tidyverse', 'lubridate')) %dopar% {

  if(i == 'all')
    ID <- unique(ID)

  out <- addlmet_fun(id = ID, flowin = CanESM2ext_sub, dtend = dtsls, subnm = i)
  return(out)

}

print(Sys.time() - strt)

addlprecipmet <- do.call('rbind', res) %>%
  rename(COMID = stid)

canesm2precdt2 <- precipcmb_fun(kradprecipmet, addlprecipmet)

save(canesm2precdt2, file = 'data/canesm2precdt2.RData', compress = 'xz')

# ccsm4dt1 get precip mets ----------------------------------------------------------------

data(comid_pnts)
data(CCSM4ext)

# input
toproc <- crossing(
  dtsl = as.Date(c('2040-07-01')),
  COMID = comid_pnts$COMID
)
ID <- toproc$COMID
dtsls <- toproc$dtsl

# subset precip daily time series by dates
CCSM4ext_sub <- CCSM4ext %>%
  filter(date <= as.Date('2040-07-01'))

##
# konrad

# setup parallel backend
cores <- detectCores() - 1
cl <- makeCluster(cores)
registerDoParallel(cl)

# log
strt <- Sys.time()

# estimate konrad
res <- foreach(i = c('x3', 'x5', 'x10', 'all'), .packages = c('tidyverse', 'lubridate')) %dopar% {

  if(i == 'all')
    ID <- unique(ID)

  out <- konradfun(id = ID, flowin = CCSM4ext_sub, dtend = dtsls, subnm = i)
  return(out)

}

print(Sys.time() - strt)

kradprecipmet <- do.call('rbind', res) %>%
  rename(COMID = stid)

##
# addl precip metrics

# setup parallel backend
cores <- detectCores() - 1
cl <- makeCluster(cores)
registerDoParallel(cl)

# log
strt <- Sys.time()

#prep site file data, ~2 hrs
res <- foreach(i = c('x3', 'x5', 'x10', 'all'), .packages = c('tidyverse', 'lubridate')) %dopar% {

  if(i == 'all')
    ID <- unique(ID)

  out <- addlmet_fun(id = ID, flowin = CCSM4ext_sub, dtend = dtsls, subnm = i)
  return(out)

}

print(Sys.time() - strt)

addlprecipmet <- do.call('rbind', res) %>%
  rename(COMID = stid)

ccsm4precdt1 <- precipcmb_fun(kradprecipmet, addlprecipmet)

save(ccsm4precdt1, file = 'data/ccsm4precdt1.RData', compress = 'xz')

# ccsm4dt2 get precip mets ----------------------------------------------------------------

data(comid_pnts)
data(CCSM4ext)

# input
toproc <- crossing(
  dtsl = as.Date(c('2100-07-01')),
  COMID = comid_pnts$COMID
)
ID <- toproc$COMID
dtsls <- toproc$dtsl

# subset precip daily time series by dates
CCSM4ext_sub <- CCSM4ext %>%
  filter(date <= as.Date('2100-07-01') & date >= as.Date('2082-07-01'))

##
# konrad

# setup parallel backend
cores <- detectCores() - 1
cl <- makeCluster(cores)
registerDoParallel(cl)

# log
strt <- Sys.time()

# estimate konrad
res <- foreach(i = c('x3', 'x5', 'x10', 'all'), .packages = c('tidyverse', 'lubridate')) %dopar% {

  if(i == 'all')
    ID <- unique(ID)

  out <- konradfun(id = ID, flowin = CCSM4ext_sub, dtend = dtsls, subnm = i)
  return(out)

}

print(Sys.time() - strt)

kradprecipmet <- do.call('rbind', res) %>%
  rename(COMID = stid)

##
# addl precip metrics

# setup parallel backend
cores <- detectCores() - 1
cl <- makeCluster(cores)
registerDoParallel(cl)

# log
strt <- Sys.time()

#prep site file data, ~2 hrs
res <- foreach(i = c('x3', 'x5', 'x10', 'all'), .packages = c('tidyverse', 'lubridate')) %dopar% {

  if(i == 'all')
    ID <- unique(ID)

  out <- addlmet_fun(id = ID, flowin = CCSM4ext_sub, dtend = dtsls, subnm = i)
  return(out)

}

print(Sys.time() - strt)

addlprecipmet <- do.call('rbind', res) %>%
  rename(COMID = stid)

ccsm4precdt2 <- precipcmb_fun(kradprecipmet, addlprecipmet)

save(ccsm4precdt2, file = 'data/ccsm4precdt2.RData', compress = 'xz')

# miroc5dt1 get precip mets ---------------------------------------------------------------

data(comid_pnts)
data(MIROC5ext)

# input
toproc <- crossing(
  dtsl = as.Date(c('2040-07-01')),
  COMID = comid_pnts$COMID
)
ID <- toproc$COMID
dtsls <- toproc$dtsl

# subset precip daily time series by dates
MIROC5ext_sub <- MIROC5ext  %>%
  filter(date <= as.Date('2040-07-01'))

##
# konrad

# setup parallel backend
cores <- detectCores() - 1
cl <- makeCluster(cores)
registerDoParallel(cl)

# log
strt <- Sys.time()

# estimate konrad
res <- foreach(i = c('x3', 'x5', 'x10', 'all'), .packages = c('tidyverse', 'lubridate')) %dopar% {
  
  if(i == 'all')
    ID <- unique(ID)
  
  out <- konradfun(id = ID, flowin = MIROC5ext_sub, dtend = dtsls, subnm = i)
  return(out)
  
}

print(Sys.time() - strt)

kradprecipmet <- do.call('rbind', res) %>%
  rename(COMID = stid)

##
# addl precip metrics

# setup parallel backend
cores <- detectCores() - 1
cl <- makeCluster(cores)
registerDoParallel(cl)

# log
strt <- Sys.time()

#prep site file data, ~2 hrs
res <- foreach(i = c('x3', 'x5', 'x10', 'all'), .packages = c('tidyverse', 'lubridate')) %dopar% {
  
  if(i == 'all')
    ID <- unique(ID) 
  
  out <- addlmet_fun(id = ID, flowin = MIROC5ext_sub, dtend = dtsls, subnm = i)
  return(out)
  
}

print(Sys.time() - strt)

addlprecipmet <- do.call('rbind', res) %>%
  rename(COMID = stid)

miroc5precdt1 <- precipcmb_fun(kradprecipmet, addlprecipmet)

save(miroc5precdt1, file = 'data/miroc5precdt1.RData', compress = 'xz')


# miroc5dt2 get precip mets ---------------------------------------------------------------

data(comid_pnts)
data(MIROC5ext)

# input
toproc <- crossing(
  dtsl = as.Date(c('2100-07-01')),
  COMID = comid_pnts$COMID
)
ID <- toproc$COMID
dtsls <- toproc$dtsl

# subset precip daily time series by dates
MIROC5ext_sub <- MIROC5ext %>%
  filter(date <= as.Date('2100-07-01') & date >= as.Date('2082-07-01'))

##
# konrad

# setup parallel backend
cores <- detectCores() - 1
cl <- makeCluster(cores)
registerDoParallel(cl)

# log
strt <- Sys.time()

# estimate konrad
res <- foreach(i = c('x3', 'x5', 'x10', 'all'), .packages = c('tidyverse', 'lubridate')) %dopar% {
  
  if(i == 'all')
    ID <- unique(ID)
  
  out <- konradfun(id = ID, flowin = MIROC5ext_sub, dtend = dtsls, subnm = i)
  return(out)
  
}

print(Sys.time() - strt)

kradprecipmet <- do.call('rbind', res) %>%
  rename(COMID = stid)

##
# addl precip metrics

# setup parallel backend
cores <- detectCores() - 1
cl <- makeCluster(cores)
registerDoParallel(cl)

# log
strt <- Sys.time()

#prep site file data, ~2 hrs
res <- foreach(i = c('x3', 'x5', 'x10', 'all'), .packages = c('tidyverse', 'lubridate')) %dopar% {
  
  if(i == 'all')
    ID <- unique(ID) 
  
  out <- addlmet_fun(id = ID, flowin = MIROC5ext_sub, dtend = dtsls, subnm = i)
  return(out)
  
}

print(Sys.time() - strt)

addlprecipmet <- do.call('rbind', res) %>%
  rename(COMID = stid)

miroc5precdt2 <- precipcmb_fun(kradprecipmet, addlprecipmet)

save(miroc5precdt2, file = 'data/miroc5precdt2.RData', compress = 'xz')


# predict future flowmetrics for 2040, canesm2 ----------------------------

data(flowmet)
data(precipmet)
data(flowmetprf)
data(canesm2precdt1)
data(comid_attsall)

# setup parallel
ncores <- detectCores() - 2
cl<-makeCluster(ncores)
registerDoParallel(cl)

canesm2flowmetdt1 <- flowmetprd_fun(flowmet, precipmet, flowmetprf, canesm2precdt1, comid_attsall)

save(canesm2flowmetdt1, file = 'data/canesm2flowmetdt1.RData', compress = 'xz')

# predict future flowmetrics for 2100, canesm2 ----------------------------

data(flowmet)
data(precipmet)
data(flowmetprf)
data(canesm2precdt2)
data(comid_attsall)

# setup parallel
ncores <- detectCores() - 2
cl<-makeCluster(ncores)
registerDoParallel(cl)

canesm2flowmetdt2 <- flowmetprd_fun(flowmet, precipmet, flowmetprf, canesm2precdt2, comid_attsall)

save(canesm2flowmetdt2, file = 'data/canesm2flowmetdt2.RData', compress = 'xz')

# predict future flowmetrics for 2040, ccsm4 ----------------------------

data(flowmet)
data(precipmet)
data(flowmetprf)
data(ccsm4precdt1)
data(comid_attsall)

# setup parallel
ncores <- detectCores() - 2
cl<-makeCluster(ncores)
registerDoParallel(cl)

ccsm4flowmetdt1 <- flowmetprd_fun(flowmet, precipmet, flowmetprf, ccsm4precdt1, comid_attsall)

save(ccsm4flowmetdt1, file = 'data/ccsm4flowmetdt1.RData', compress = 'xz')

# predict future flowmetrics for 2100, ccsm4 ----------------------------

data(flowmet)
data(precipmet)
data(flowmetprf)
data(ccsm4precdt2)
data(comid_attsall)

# setup parallel
ncores <- detectCores() - 2
cl<-makeCluster(ncores)
registerDoParallel(cl)

ccsm4flowmetdt2 <- flowmetprd_fun(flowmet, precipmet, flowmetprf, ccsm4precdt2, comid_attsall)

save(ccsm4flowmetdt2, file = 'data/ccsm4flowmetdt2.RData', compress = 'xz')

# predict future flowmetrics for 2040, miroc5 ----------------------------

data(flowmet)
data(precipmet)
data(flowmetprf)
data(miroc5precdt1)
data(comid_attsall)

# setup parallel
ncores <- detectCores() - 2
cl<-makeCluster(ncores)
registerDoParallel(cl)

miroc5flowmetdt1 <- flowmetprd_fun(flowmet, precipmet, flowmetprf, miroc5precdt1, comid_attsall)

save(miroc5flowmetdt1, file = 'data/miroc5flowmetdt1.RData', compress = 'xz')

# predict future flowmetrics for 2100, miroc5 ----------------------------

data(flowmet)
data(precipmet)
data(flowmetprf)
data(miroc5precdt2)
data(comid_attsall)

# setup parallel
ncores <- detectCores() - 2
cl<-makeCluster(ncores)
registerDoParallel(cl)

miroc5flowmetdt2 <- flowmetprd_fun(flowmet, precipmet, flowmetprf, miroc5precdt2, comid_attsall)

save(miroc5flowmetdt2, file = 'data/miroc5flowmetdt2.RData', compress = 'xz')