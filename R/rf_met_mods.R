######
# random forest models of flow metrics

library(tidyverse)
library(lubridate)
library(sf)
library(randomForest)

data(flowmet)
data(yrprcp)
data(comid_atts)

# metsel <- 'x5_HighDur'

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
  # filter(var %in% metsel) %>% 
  left_join(comid_atts, by = 'COMID') %>% 
  left_join(yrprcp, by = 'yr') %>% 
  group_by(var, mo, catprcp) %>% 
  nest

# create models
mods <- tomod %>% 
  mutate(
    ests = pmap(list(var, mo, catprcp, data), function(var, mo, catprcp, data){
      
      cat(var, mo, as.character(catprcp), '\n')
      
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
      
    })
  )

# final output
flowmetest <- mods %>% 
  select(-data) %>% 
  unnest(ests)

save(flowmetest, file = 'data/flowmetest.RData', compress = 'xz')