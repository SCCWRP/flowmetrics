#Creating additional metrics with Kelly's modeled flow

library(tidyverse)

# run line 1-209 from KonradMetrics_Extrapolation to load flow data and site file data 
# everytthing above this:
##########################################################################################
#CENSOR FLOW RECORDS (q) FOR SELECTED SITES AND WATER YEARS
# doesn't matter which year
# setting up dataframe with flow values, date, year, and month columns

test<-q
test$date<-row.names(q)
test$month<-month(test$date)
test$year<-year(test$date)


# get list of dates that we want to make this calulation for
sites<-sites[,c(1,5,6)]
sites<-unique(sites)
names(sites)[1]<-"SITE"


#make flow in long format and then group by location, year, month

newdat<-test %>% 
  gather("SITE","flow",`2`:`69`)



##############################################   Monthly mean   ########################################################


# calculating monthly mean
newdat<-newdat %>% 
  group_by(SITE,year, month)

monthly_mean<-summarize(newdat, flow = mean(flow))

out <- NULL  
for (i in 1:nrow(sites)){
  
  cat(i, '\t')
  
  rw <- sites[i, ]
  site <- rw$SITE
  
  # date objects
  dt <- paste(rw$endyr, rw$endmn, '15', sep = '-') %>% 
    ymd
  dtpri <- dt - (11 * 31)
  
  # staring year mo
  stryr <- rw$endyr
  strmo <- rw$endmn
  
  # prior year mo
  priyr <- year(dtpri)
  primo <- month(dtpri)
  
  monthsel <- c(primo:12, 1:strmo)
  # starting flow
  startind <- with(monthly_mean, which(year == stryr & month == strmo & SITE == site))
  endinind <- with(monthly_mean, which(year == priyr & month == primo & SITE == site))
  
  # output
  flows <- monthly_mean[endinind:startind, ] %>% 
    mutate(month = month(month, label = T)) %>% 
    arrange(month) %>% 
    ungroup %>% 
    select(month, flow) %>% 
    spread(month, flow)
  
  out <- rbind(out, flows)
  
}

sitesflo <- cbind(sites, out)

write.csv(sitesflo, file="C:/Users/JennyT/Documents/LitReview/RB4/WorkingData_3-16-18/MetricsForMarcus/MonthlyMeansModeled.csv")

########################################################################################################



#richards baker index past 3, 5,10, and all years
#rerun lines 1-198 in the KonradMetrics_Jenny

test<-q
test$date<-row.names(q)
test$month<-month(test$date)
test$year<-year(test$date)

sites<-sites[,c(1,5,6,7)]
sites<-unique(sites)
names(sites)[1]<-"SITE"


#make flow in long format and then group by location, year, month

newdat<-test %>% 
  gather("SITE","flow",`2`:`69`)

yrs <- c(3, 5, 10)

final <- NULL  
for (i in 1:nrow(sites)){
  
  cat(i, '\t')
  
  rw <- sites[i, ]
  site <- rw$SITE
  
  # date objects
  dt <- paste(rw$endyr, rw$endmn, rw$enddy, sep = '-') %>% 
    ymd
  dtpri <- dt - (yrs*(12 * 30.4)) 
  
  # flow slices
  flows <- newdat %>% 
    filter(SITE %in% site)
  
  rbis <- sapply(dtpri, function(x){
    
    if(x < min(flows$date))
      return(NA)
    
    flosel <- flows %>% 
      filter(date <= dt & date >= x) %>% 
      pull(flow) 
    
    rbiout <- sum(abs(diff(flosel)))/sum(flosel)
    
    # for zero flow
    if(is.nan(rbiout)) 
      rbiout <- 0
    
    return(rbiout)
    
  })
  
  final <- rbind(final, rbis)
  
}

sitesflo <- cbind(sites, final)
names(sitesflo)[5:7]<-c("3yrRBI", "5yrRBI", "10yrRBI")

write.csv(sitesflo, file="C:/Users/JennyT/Documents/LitReview/RB4/WorkingData_3-16-18/MetricsForMarcus/RBIModeled.csv")


########### RBI all years###############################################################################################

#not sure if this is right because stable streams were given high values and vice versa..
newdat2<-newdat %>% 
  group_by(SITE)

RBIall<-summarize(newdat2, rbi = sum(abs(diff(flow)))/sum(flow))

write.csv(RBIall, file="C:/Users/JennyT/Documents/LitReview/RB4/WorkingData_3-16-18/MetricsForMarcus/RBIbySiteModeled.csv")



###############################################################################################################

#storm frequency

#calculating x-year storm magnitude
#10 year flow: avg of three highest peaks in 3o yr record
#5 year flow: avg of 6 highest peaks in 30 year record
#2 year flow: avg of 15 highest peaks in 30 year record


# Time since last 2,5, and 10 year storms 
# Calculate days from date of species observation to the most recent storm.

#no need to rerun anything, just run code here

strmpks <- newdat %>%
  group_by(SITE) %>% 
  nest %>% 
  mutate(
    strms = map(data, function(x){
      
      # get storm peaks
      pks <- x %>% 
        mutate(
          date = ymd(date),
          peaks = c(diff(-flow), NA),
          peaks = c(NA, diff(sign(peaks)))
        ) %>% 
        filter(peaks == 2) %>% 
        arrange(-flow) %>% 
        pull(flow)
      
      # get magniude of storm types
      tenyr <- mean(pks[1:3])
      fivyr <- mean(pks[4:9])
      twoyr <- mean(pks[10:24])
      
      # format output
      out <- data.frame(tenyr = tenyr, fivyr = fivyr, twoyr = twoyr)
      
      return(out)
      
    })
  ) %>% 
  select(-data) %>% 
  unnest %>% 
  gather('strm', 'mags', -SITE)

toiter <- sites %>% 
  left_join(strmpks, by = 'SITE')

final <- NULL

for(i in 1:nrow(toiter)){
  
  cat(i, '\n')
  
  # get row to evaluate
  rw <- toiter[i, ]
  site <- rw$SITE
  strm <- rw$strm
  
  # date objects
  dt <- paste(rw$endyr, rw$endmn, rw$enddy, sep = '-') %>% 
    ymd
  
  # pull out flow for the station before and up to observation date
  flows <- newdat %>% 
    filter(SITE %in% site) %>% 
    filter(date <= dt)
  
  if(strm == 'tenyr'){
    
    # find index when flow was above or equal to storm mag
    strmind <- which(flows$flow >= rw$mags)[1]
    
  } else {
    
    magspri <- toiter[i - 1, 'mags']
    
    # find index when flow was above or equal to storm mag
    strmind <- rev(which(flows$flow >= rw$mags & flows$flow < magspri))[1]
    
  }
  
  # if none found, NA, otherwise, count back number of days
  if(is.na(strmind)) 
    dys <- NA
  else 
    dys <- nrow(flows) - strmind
  
  cat('\t', as.character(rw), '\n')
  cat('\t', as.character(flows[strmind, ]),'\n')
  cat('\t', dys, '\n')
  
  # append to output
  final <- c(final, dys)
  
}

strmcnts <- toiter %>% 
  mutate(
    cnts = final
  ) %>% 
  select(-mags) %>% 
  spread(strm, cnts)


write.csv(strmcnts, file="C:/Users/JennyT/Documents/LitReview/RB4/WorkingData_3-16-18/MetricsForMarcus/StormFreqModeled.csv")







########### join additional metrics together ################


#clear object list





strm<- read.csv("C:/Users/JennyT/Documents/LitReview/RB4/WorkingData_3-16-18/MetricsForMarcus/StormFreqModeled.csv") %>% 
  select(-1)
RBIsite<- read.csv("C:/Users/JennyT/Documents/LitReview/RB4/WorkingData_3-16-18/MetricsForMarcus/RBIbySiteModeled.csv") %>% 
  select(-1)
RBI<- read.csv("C:/Users/JennyT/Documents/LitReview/RB4/WorkingData_3-16-18/MetricsForMarcus/RBIModeled.csv") %>% 
  select(-1)
mth<- read.csv("C:/Users/JennyT/Documents/LitReview/RB4/WorkingData_3-16-18/MetricsForMarcus/MonthlyMeansModeled.csv") %>% 
  select(-1)

test<- left_join(strm, RBI, by = c("SITE", "endyr", "endmn", "enddy"))
test<- left_join(test, RBIsite, by = "SITE")
test<- left_join(test, mth, by = c("SITE", "endyr", "endmn"))

test$date<- mdy(paste(test$endmn, test$enddy, test$endyr, sep = "/"))
test<- test %>% 
  select(-endyr, -endmn, -enddy)

#COMIDS
ID<-read_excel("C:/Users/JennyT/Documents/LitReview/RB4/WorkingData_3-16-18/MetricsForMarcus/COMID_File.xlsx") %>% 
  select(1:2) %>% 
  filter(watershedID != 51)
names(ID)[2]<- "SITE"

test<- left_join(test, ID, by = "SITE")
test<- test %>% 
  select(date, COMID, SITE,2:20)

write.csv(test, file = "C:/Users/JennyT/Documents/LitReview/RB4/WorkingData_3-16-18/MetricsForMarcus/additional_metrics.csv")



