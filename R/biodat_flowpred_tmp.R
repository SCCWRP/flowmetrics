######
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

data(comid_pnts)
data(biodat)
data(flowmetprf)
data(bsext)

## 
# observed bio data

# # july, top ten, validation, rsqr_ave > 0.5
# prfeval <- flowmetprf %>% 
#   filter(mo %in% 7) %>% 
#   filter(modtyp %in% 'prdimp') %>% 
#   filter(set %in% 'val') %>% 
#   dplyr::select(-data, -mo, -set, -modtyp, -rmse) %>% 
#   group_by(var) %>% 
#   nest %>% 
#   mutate(
#     rsqr_ave = purrr::map(data, function(x) mean(x$rsqr, na.rm = T)), 
#     impvars = purrr::map(data, function(x) unique(unlist(x$impvars)))
#   ) %>% 
#   dplyr::select(-data) %>% 
#   unnest(rsqr_ave) %>% 
#   filter(rsqr_ave > 0.5)
# 
# # precip metrics to estimate
# mets <- prfeval %>% 
#   pull(var)

######
# pull precip metrics where we have biodata 

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

######
# random forest models of flow metrics, predicted

data(flowmet)
data(precipmet)
data(flowmetprf)
data(bioprecipmet)
data(comid_attsall)
data(comid_atts)

# top predictors for each metric, july only
# all important predictors across the five folds are combined
impvarsall <- flowmetprf %>%
  filter(mo %in% 7) %>%
  filter(modtyp %in% 'prdimp') %>%
  filter(set %in% 'val') %>%
  dplyr::select(-data, -mo, -set, -modtyp, -rmse, -rsqr) %>%
  group_by(var) %>%
  nest %>%
  mutate(
    impvars = purrr::map(data, function(x) unique(unlist(x$impvars)))
  ) %>%
  dplyr::select(-data)

# static predictors
static <- comid_attsall %>%
  st_set_geometry(NULL)

# setup data to model
tomod <- flowmet %>%
  dplyr::select(-tenyr) %>% # do not model tenyr
  mutate(
    mo = month(date)
  ) %>%
  group_by(mo) %>% 
  mutate(folds = sample(1:5, length(mo), replace = T)) %>%
  ungroup %>%
  filter(mo %in% 7) %>% 
  gather('var', 'val', -watershedID, -COMID, -date, -mo, -folds) %>%
  left_join(comid_atts, by = 'COMID') %>%
  left_join(precipmet, by = c('COMID', 'date')) %>%
  group_by(var, mo) %>%
  nest

# prediction data from bio precipmetrics
# join with comid_atts_bio_nogm
biopreddat <- bioprecipmet %>% 
  left_join(static, by = 'COMID')

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
  data <- tomod$data[[rw]]
  
  # import predicors for the var
  impvars <- impvarsall %>% 
    filter(var %in% !!var) %>% 
    pull(impvars) %>% 
    unlist

  # use only one fold
  fld <- 1

  # calibration data
  calset <- data %>%
    filter(!folds %in% fld)

  # model formula, from top predictors
  frmimp <- impvars %>%
    paste(collapse = '+') %>%
    paste0('val~', .) %>%
    formula

  # create model, from top ten
  modimp <- randomForest(frmimp, data = calset, ntree = 500, importance = TRUE, na.action = na.omit, keep.inbag = TRUE)

  # data to predict
  toprd <- biopreddat[, impvars]
  
  # predictions, all comid attributes
  prdimp <- predict(modimp, newdata = toprd, predict.all = T)
  bnds <- apply(prdimp$individual, 1, function(x) quantile(x, probs = c(0.1, 0.9), na.rm = T))
  cv <- apply(prdimp$individual, 1, function(x) sd(x, na.rm = T)/mean(x, na.rm = T))

  # combine output, estimates, lo, hi, cv, dataset
  out <- tibble(
    COMID = biopreddat$COMID,
    date = biopreddat$date, 
    dtsl = biopreddat$dtsl, 
    est = prdimp$aggregate,
    lov= bnds[1, ],
    hiv= bnds[2, ],
    cv = cv
    ) %>%
    mutate(set = ifelse(COMID %in% calset$COMID, 'cal', 'notcal'))

  return(out)

}

# final output
bioflowmetest <- tomod %>%
  dplyr::select(-data) %>%
  bind_cols(., enframe(modsest)) %>%
  dplyr::select(-name) %>%
  unnest(value) 

# join with biology (only date, COMID were modelled, species doesn't matter)
bioflowmetest <- bioflowmetest %>% 
  left_join(biodat, ., by = c('COMID', 'date')) %>% 
  dplyr::select(name, COMID, date, var, est) %>% 
  spread(var, est)
              
save(bioflowmetest, file = 'data/bioflowmetest.RData', compress = 'xz')
