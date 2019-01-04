library(h5)

# setwd to rcp45 scenario
setwd('L:/Flow ecology and climate change_ES/Data/RawData/CCSM4/rcp45')

# get file list of yearly estimates
fls <- dir()

# get first file
fl <- fls[1] 

# extract contents to wd, subdir tmp
untar(fl, exdir = 'tmp')

# get h5 file names in extracted directory
h5fls <- list.files('tmp', recursive = T, full.names = T)

# select one h5 file, open connection
h5fl <- h5fls[1]
h5flcon <- h5file(name = h5fl, mode = "a")

# read dataset, 3hr x 1600 x 2400
ppt <- readDataSet(h5flcon['PPT'])

# plot hour 1
image(ppt[1, ,])

# questions
# what are precip units, mm?
# what is bounding box? 
# what are three hour time steps? 
