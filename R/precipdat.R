library(h5)
library(tidyverse)
library(sf)
library(raster)
library(foreach)
library(doParallel)

utmprj <- "+proj=utm +zone=11 +datum=NAD83 +units=m +no_defs"
decprj <- "+proj=longlat +ellps=WGS84 +datum=WGS84 +no_defs"

data(comid_pnts)

# reproject
comid_sel <- comid_pnts %>% 
  st_transform(decprj)

# lat and lon
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
  select(date, COMID, dly_prp)
save(bsext, file = 'data/bsext.RData', compress = 'xz')
