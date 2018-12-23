#######
# prep data frame for clustering stream reaches based on anthropogenic modification
# we are not modelling flow metrics on streams that are modified (e.g., dominated by urban land use, dams present, etc.)

library(tidyverse)
library(sf)
library(caret)

# streamcat and NHD data
dam<-read.csv("//172.16.1.5/Data/MarcusBeck/FromJenny/Dams_Region18.csv")
impervious<-read.csv("//172.16.1.5/Data/MarcusBeck/FromJenny/ImperviousSurfaces2011_CA.csv")
nlcd2011<-read.csv("//172.16.1.5/Data/MarcusBeck/FromJenny/NLCD2011_Region18.csv")
roadden<-read.csv("//172.16.1.5/Data/MarcusBeck/FromJenny/RoadDensity_Region18.csv")
NHDplus<-st_read("//172.16.1.5/Data/MarcusBeck/FromJenny/NHDFlowline_Clip_NAD1983_UTMzone11.shp") %>% 
  st_zm() # drops z geometry

# merge streamcat with nhdplus geometry
NHD_dam<-merge(NHDplus,dam, by="COMID")
NHD_dam<-select(NHD_dam, "COMID", "CatAreaSqKm", "WsAreaSqKm", "DamDensWs", "DamNrmStorWs", "geometry")
NHD_dam_imperv<-merge(NHD_dam, impervious, by="COMID")
NHD_dam_imperv<-select(NHD_dam_imperv, "COMID","CatAreaSqKm.x","WsAreaSqKm.x","DamDensWs","DamNrmStorWs", "PctImp2011Cat", "geometry" )
NHD_dam_imperv_nldc2011<-merge(NHD_dam_imperv, nlcd2011, by="COMID")
NHD_dam_imperv_nldc2011<-select(NHD_dam_imperv_nldc2011,"COMID","CatAreaSqKm.x","WsAreaSqKm.x","DamDensWs",
                                   "DamNrmStorWs", "PctImp2011Cat", "PctUrbOp2011Cat", "PctUrbLo2011Cat" ,
                                   "PctUrbMd2011Cat", "PctUrbHi2011Cat","geometry" )
NHD_dam_imperv_nldc2011_roadDen<-merge(NHD_dam_imperv_nldc2011,roadden, by="COMID")
NHD_dam_imperv_nldc2011_roadDen<-select(NHD_dam_imperv_nldc2011_roadDen,"COMID","CatAreaSqKm.x","WsAreaSqKm.x","DamDensWs",
                                           "DamNrmStorWs", "PctImp2011Cat", "PctUrbOp2011Cat", "PctUrbLo2011Cat" ,
                                           "PctUrbMd2011Cat", "PctUrbHi2011Cat",  "RdDensCat" ,"geometry" )
NHD_StreamCat<-select(NHD_dam_imperv_nldc2011_roadDen, -CatAreaSqKm.x, -WsAreaSqKm.x)

############################################################

# prepping df and clustering

distanceMatric<-dist(dat[,4:7], method = "euclidean", diag = FALSE, upper=FALSE)
clusters<-hclust(distanceMatric)
plot(clusters)
clusters
clusterCut<-cutree(clusters, 5)
table(clusterCut)

#bind clusters to dataset
dat2<-cbind(NHD_StreamCat, clusterCut)
plot(dat2["clusterCut"], main="Stream Catorgorized by Urban Development")

#Making a variable for whether or not there is a reservoir in the watershed - we want to remove these also.
dat2$dam[dat2$DamDensWs==0]<-0
dat2$dam[dat2$DamDensWs>0]<-1
plot(dat2["dam"], main="Stream Catorgorized by Dam Presence in Watershed")

#write shapefile to folder
# st_write(dat2, "//172.16.1.5/Data/MarcusBeck/FromJenny/COMID clustering.shp")



###################################################################################################

#Prep data for random forest prediction of stream metrics
#in this step I merged the NHDflowline data with the stream cat csv's below. after each merge, 
#some variables were removed either because they were repetitive, or because they did not have any 
#variation - this was the first step in narrowing down the variables for the random forest

elevation<-read.csv("//172.16.1.5/Data/MarcusBeck/FromJenny/Elevation_Region18.csv")
GeoChem<-read.csv("//172.16.1.5/Data/MarcusBeck/FromJenny/GeoChemPhys3_Region18.csv")
lithology<-read.csv("//172.16.1.5/Data/MarcusBeck/FromJenny/Lithology_Region18.csv")
prism<-read.csv("//172.16.1.5/Data/MarcusBeck/FromJenny/PRISM_1981_2010_Region18.csv")
runoff<-read.csv("//172.16.1.5/Data/MarcusBeck/FromJenny/Runoff_Region18.csv")
statsgo<-read.csv("//172.16.1.5/Data/MarcusBeck/FromJenny/STATSGO_Set2_Region18.csv")
nlcd2011<-read.csv("//172.16.1.5/Data/MarcusBeck/FromJenny/NLCD2011_Region18.csv")


NHD_merge<-merge(dat2, elevation, by="COMID")
NHD_merge<-select(NHD_merge, -DamDensWs, -DamNrmStorWs, -RdDensCat, -CatPctFull, -WsPctFull)
NHD_merge<-merge(NHD_merge, GeoChem, by="COMID")
NHD_merge<-select(NHD_merge, -(CatAreaSqKm.y:WsPctFull))
NHD_merge<-merge(NHD_merge, lithology, by="COMID")
#preliminary guess at which variables to remove based on the ones with similar values across all COMIDs
test<-NHD_merge
test$clusterCut<-as.numeric(test$clusterCut)
test<-data.frame(test)
test<-select(test, -geometry, -COMID)
z<-nearZeroVar(test)#gives the variables with low variance based on default values
z+1 #because we removed the comid everything will be up one variable
NHD_merge<-NHD_merge[,-c(17, 18, 19, 21, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 35, 36, 37, 39, 41, 42, 43, 44,
                         45, 46, 47, 48, 49, 50, 51, 53, 54)]
NHD_merge<-select(NHD_merge, -CatAreaSqKm.x, -WsAreaSqKm.x)
NHD_merge<-merge(NHD_merge, prism, by="COMID")
NHD_merge<-NHD_merge[,-c(23,24)]
NHD_merge<-merge(NHD_merge, runoff, by="COMID")
NHD_merge<-NHD_merge[,-c(31:34)]
NHD_merge<-merge(NHD_merge, statsgo, by="COMID")
NHD_merge<-NHD_merge[,-c(33:36)]             
NHD_merge<-merge(NHD_merge, nlcd2011, by="COMID")

test2<-NHD_merge
test2$clusterCut<-as.numeric(test2$clusterCut)
test2<-data.frame(test2)
test2<-select(test2, -geometry, -COMID)
z1<-nearZeroVar(test2)#gives the variables with low variance based on default values
z1+1
NHD_merge<-NHD_merge[,-c(43, 44, 46, 52, 57, 58, 62)]#getting rid of the nearZerVar variables
NHD_merge<-NHD_merge[,-(3:6)]#getting rid of replicated nlcd2011 urban variables

#remove the rows with clustercut !=1 and dam=1 because these reaches will not be predicted
#redo the nearZeroVar function to remove any last variables that do not show variance in the remaining 
#reaches
NHDpredict<-NHD_merge[NHD_merge$dam==0,]
NHDpredict<-NHDpredict[NHDpredict$clusterCut==1,]
NHDpredict<-NHDpredict[,-c(3,4)]#remove dam and clusterCut

test3<-NHDpredict
test3<-data.frame(test3)
test3<-select(test3, -geometry, -COMID)
z2<-nearZeroVar(test3)#gives the variables with low variance based on default values
z2+1
NHDpredict<-NHDpredict[,-c(37, 41, 48, 53, 55)]
NHDpredict<-select(NHDpredict, -CatAreaSqKm.x, -WsAreaSqKm.x, -CatAreaSqKm.y, -WsAreaSqKm.y)

comid_atts <- NHDpredict
save(comid_atts, file = 'data/comid_atts.RData', compress = 'xz')

#write shapefile to folder
# st_write(NHDpredict, "//172.16.1.5/Data/MarcusBeck/FromJenny/COMID attributes.shp")

#################################################################################################

# #investigating relationships between runoffWatershed variable and the others
# test4<-data.frame(NHDpredict)
# test4<-select(test4, -COMID, -geometry)
# par(mfrow=c(1,1), mar=c(5,4,2,2))
# hist(test4$RunoffCat, xlim = c(0,1500))
# plot(test4$RunoffCat, test4$Precip8110Cat)
# with(test4, plot(ElevCat, ElevWs))
# with(test4, plot(HydrlCondCat, HydrlCondWs))    
# with(test4, plot(PctNonCarbResidCat, PctNonCarbResidWs))
# with(test4, plot(PctSilicicCat, PctSilicicWs))
# with(test4, plot(ElevWs, PctUrbLo2011Cat.y))
# 
# VariableNAs<-data.frame(colSums(is.na(test4)))#calculate number of NAs for each variable
# #remove the observations with NA so we can do a correlation matrix
# test5<-test4[complete.cases(test4),]
# VariableNAs<-data.frame(colSums(is.na(test5)))#check to make sure 0 for each column
# 
# corMatrix<-cor(test5) #correlation matrix for each variable
# correlated<-findCorrelation(corMatrix)#returns columns that have a correlation with another variable>.9
# #remove runoff because its flow per unit area, 
# names(test5[,c(3, 18, 14, 15, 12,  1, 27,  6, 10, 20)])#returns names of variables from line above
