library(h5)
library(tidyverse)
library(sf)
library(raster)

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
fls <- list.files('D:/Baseline', recursive = T, full.names = T)

# output
res <- list()
strt<-Sys.time()

# process
for (i in seq_along(fls)){  
  
  # counter
  cat(i, '\n')
  print(Sys.time()-strt)
  
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
  
  # append to output
  res <- c(res, list(dly))
  
  # save every 1000 files
  if(i %% 1000 == 0)
    save(res, file = 'data/res.RData', compress = 'xz')
  
}

# combine output to save
names(res) <- basename(fls)
bsext <- res %>% 
  enframe %>% 
  unnest
save(bsext, file = 'data/bsext.RData', compress = 'xz')
