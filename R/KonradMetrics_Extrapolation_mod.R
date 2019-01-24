##This script calculates flow metrics based on exact day of species record and whatever timeframe is specified in the site_file
#create site and date csv

library(tidyverse)
library(lubridate)
library(foreach)
library(doParallel)

data(bsext)
data(flowmet)

source('R/konradfun.R')

# select master ID
ID <- unique(flowmet$COMID) %>% 
  sort

# setup parallel backend
cores <- detectCores() - 2
cl <- makeCluster(cores)
registerDoParallel(cl)

#prep site file data
res <- foreach(i = c('x3', 'x5', 'x10', 'all'), .packages = c('tidyverse', 'lubridate')) %dopar% {

  konradfun(id = ID, flowin = bsext, subnm = i)
  
}

kradprecipmet <- do.call('rbind', res) %>% 
  rename(COMID = stid)
save(kradprecipmet, file = 'data/kradprecipmet.RData', compress = 'xz')
