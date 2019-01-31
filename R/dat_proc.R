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

##
# flow metrics to predict

flowmet <- read.csv('raw/final_metrics.csv', stringsAsFactors = F) %>% 
  dplyr::select(SITE.x, COMID, date, tenyr, twoyr, X5yrRBI, x3_SFR, all_R10D.5, x3_QmaxIDR, x5_RecessMaxLength, all_LowDur, x5_HighDur, x3_HighDur, x10_HighNum, all_MedianNoFlowDays, all_Qmax, all_Q99) %>% 
  rename(
    watershedID = SITE.x
  ) %>% 
  mutate(
    date = ymd(date)
  )

save(flowmet, file = 'data/flowmet.RData', compress = 'xz')

######
# COMID points as sf object, all COMIDs in study area including dammed/urban and no bio data

data(flowmet)

comid_pnts <- st_read('L:/Flow ecology and climate change_ES/Jenny/AirTemp/COMID_to_Point.shp') %>% 
  dplyr::select(COMID) %>% 
  st_zm()
save(comid_pnts, file = 'data/comid_pnts.RData', compress = 'xz')

######
# extract hires simulated precip data by COMID pnts

utmprj <- "+proj=utm +zone=11 +datum=NAD83 +units=m +no_defs"
decprj <- "+proj=longlat +ellps=WGS84 +datum=WGS84 +no_defs"

data(comid_pnts)

# reproject
comid_sel <- comid_pnts %>% 
  st_transform(decprj)

# lat and lon bounding box
# lats <- readLines('L:/Flow ecology and climate change_ES/Data/RawData/Lat.dat') %>% 
#   strsplit('\\s+') %>% 
#   unlist %>% 
#   unique %>% 
#   as.numeric %>% 
#   range(na.rm = T)
# lons <- readLines('L:/Flow ecology and climate change_ES/Data/RawData/Lon.dat') %>% 
#   strsplit('\\s+') %>% 
#   unlist %>% 
#   unique %>% 
#   as.numeric %>% 
#   range(na.rm = T)
lats <- c(33.5, 35.01101)
lons <- c(-119.70, -117.37)

# get recursive file list, daily for each water year
fls <- list.files('D:/PPT/Baseline', recursive = T, full.names = T)

# setup parallel backend
ncores <- detectCores() - 1  
cl<-makeCluster(ncores)
registerDoParallel(cl)
strt<-Sys.time()

# process
res <- foreach(i = seq_along(fls), .packages = c('tidyverse', 'sf', 'raster', 'h5')) %dopar% {
  
  # log
  sink('log.txt')
  cat(i, 'of', length(fls), '\n')
  print(Sys.time()-strt)
  sink()
  
  # select one h5 file, open connection
  h5flcon <- h5file(name = fls[i], mode = "a")
  
  # read dataset, 8(3hr) x 1600 x 2400
  # rearrange rows, get in correct order for array
  # make raster brick
  ppt <- readDataSet(h5flcon['PPT']) %>% 
    .[, ncol(.):1, ] 
  nr <- ncol(ppt)
  nc <- dim(ppt)[3]
  nz <- nrow(ppt)
  ppt <- sapply(1:nrow(ppt), function(x) ppt[x, , ], simplify = F)
  ppt <- array(unlist(ppt), dim = c(nr, nc, nz)) %>%
    brick(xmn = lons[1], xmx = lons[2], ymn = lats[1], ymx = lats[2], crs = CRS(decprj))
  
  # extract three hour estimates for the day
  # get daily total
  dly <- ppt %>%  
    extract(comid_sel) %>% 
    rowSums(na.rm = T) %>% 
    tibble(COMID = comid_pnts$COMID, dly_prp = .)
  
  # close the connection
  h5close(h5flcon)
  
  return(dly)
  
}

# combine output to save
names(res) <- basename(fls)
bsext <- res %>% 
  enframe %>% 
  unnest %>% 
  mutate(
    date = gsub('^.*\\_([0-9]+)\\.h5$', '\\1', name),
    date = lubridate::ymd(date)
  ) %>% 
  dplyr::select(date, COMID, dly_prp)
save(bsext, file = 'data/bsext.RData', compress = 'xz')

######
# get precip metrics for predictive modelling

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
  spread(met, val)

save(precipmet, file = 'data/precipmet.RData', compress = 'xz')


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
  dplyr::select(-totprcp)

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
flowmetest <- tomod %>% 
  dplyr::select(-data) %>% 
  bind_cols(., enframe(modsprf)) %>% 
  dplyr::select(-name) %>% 
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
  dplyr::select(-totprcp)

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
  dplyr::select(-data) %>% 
  bind_cols(., enframe(modsest)) %>% 
  dplyr::select(-name) %>% 
  unnest(value)

# get comid geometry
comid_atts_gm <- comid_atts %>% 
  dplyr::select(COMID)

# join geometry
flowmetest <- flowmetest %>% 
  left_join(comid_atts_gm, by = 'COMID') %>% 
  st_as_sf()

save(flowmetest, file = 'data/flowmetest.RData', compress = 'xz')
