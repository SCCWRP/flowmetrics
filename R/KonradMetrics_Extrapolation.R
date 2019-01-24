##This script calculates flow metrics based on exact day of species record and whatever timeframe is specified in the site_file
#create site and date csv



library(sf)
library(plyr)
library(dplyr)
library(readxl)
library(lubridate)


#prep site file data


#COMIDS
ID<-read_excel("raw/COMID_File.xlsx") %>% 
  select(2) %>% 
 filter(watershedID != 51)

#3 year data
x3yr<-seq.Date(from = as.Date("1985/10/1"), to = as.Date("2014/9/30"), "quarter")

#5 year data
x5yr<-seq.Date(from = as.Date("1987/10/1"), to = as.Date("2014/9/30"), "quarter")

#10  year data
x10yr<-seq.Date(from = as.Date("1992/10/1"), to = as.Date("2014/9/30"), "quarter")




#make dataframe for each time frame
thre<- expand.grid(watershedID=ID$watershedID , date = x3yr)
thre<- thre[order(thre$watershedID),]

fiv<- expand.grid(watershedID=ID$watershedID , date = x5yr)
fiv<- fiv[order(fiv$watershedID),]

ten<- expand.grid(watershedID=ID$watershedID , date = x10yr)
ten<- ten[order(ten$watershedID),]





#Create "sitefile"
sitefile_create<- data.frame(
  "stid" = thre$watershedID,
  "styr" = NA,
  "stmn" = as.numeric(month(thre$date)),
  "stdy" = as.numeric(day(thre$date)), 
  "endyr" = as.numeric(year(thre$date)), 
  "endmn" = as.numeric(month(thre$date)),
  "enddy" = as.numeric(day(thre$date)),
  "minyr" = 2,
  "mindays" = 200,
  "lowflowyear" = 4)
sitefile_create<-mutate(sitefile_create, styr=endyr-3)  ## subtract  number of years
sitefile_create$stid<-as.numeric(sitefile_create$stid)


#Create properly formated flow file
qin_create <- read.csv(
  "raw/From Kelly_MERGE_DailyAverages_ws001to069.csv",
  check.names = FALSE, header = TRUE)
head(qin_create) 
names(qin_create)[1] <- "Date"
names(qin_create)[2:68]<-c(1,2,3,4,5,6,7,8,9,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,
                           29,30,31,32,33,34,35,36,37,38,39,40,41,42,43,44,45,46,47,48,49,50,52,53,
                           54,55,56,57,58,59,60,61,62,63,64,65,66,67,68,69) 
qin_create<-select(qin_create, -`1`)
qin_create$Date <- as.POSIXct(qin_create$Date, format = "%m/%d/%Y", origin="1970-01-01")
qin_create1 <- qin_create[-1]


#only  use this one for the all year
#Create "sitefile"
sitefile_create<- data.frame(
  "stid" = names(qin_create1),
  "styr" = as.numeric(format(qin_create[366,1], "%Y")),
  "stmn" = as.numeric(format(qin_create[1,1], "%m")),
  "stdy" = as.numeric(format(qin_create[1,1], "%d")),
  "endyr" = as.numeric(format(qin_create[nrow(qin_create),1], "%Y")),
  "endmn" = as.numeric(format(qin_create[nrow(qin_create),1], "%m")),
  "enddy" = as.numeric(format(qin_create[nrow(qin_create),1], "%d")),
  "mindays" = 200,
  "minyr" = 2,
  "lowflowyear" = 4)


#write CSVs
write.csv(qin_create, file = "konrad/flowimp.csv", row.names = FALSE)


write.csv(sitefile_create, file = "konrad/sitefile.csv", row.names = FALSE)




######################################Defining Arguments######################################


datadir <- "konrad"
outdir <- "konrad"
InputOption <- 'csv'
ws<- 'test'
InFile <- 'flowimp.csv'
SiteFile <- 'sitefile.csv' 
StartWY <- 1991
EndWY <- 2014
LowFlowMonth<-4
MinDays <-200
MinYears <- 2
UnitConvert<-1/35.31



#####################Begin Konrad Script###################################

  #flowmetrics
  #An R function used to calculate streamflow metrics from a table of daily streamflow.
  #
  #############################################################################
  #ARGUMENTS:
  #datadir, DIRECTORY WITH STREAMFLOW DATA FILES;
  #outdir, DIRECTORY WHERE OUTPUT ARE WRITTEN
  #InputOption, FORMAT OF INPUT DATA, EITHER 'csv' OR 'rws'
  #ws, WORKSPACE IDENTIFIER (e.g., HUC17);
  #InFile, NAME OF FILE WITH STREAMFLOW DATA;
  #SiteFile, NAME OF FILE WITH LIST OF SITES AND PARAMETERS - OPTIONAL, USE 'NA' IF THERE IS NOT A FILE;
  #StartWY, EARLIEST WATER YEAR OF ANY SITE;
  #EndWY, LASTEST WATER YEAR OF ANY SITE;
  #LowFlowMonth FIRST MONTH OF 'LOW FLOW YEAR', USE 4 FOR APRIL;
  #MinDays, CRITERIA FOR PROCESSING EACH YEAR;
  #MinYears, CRITERIA FOR PROCESSING EACH SITE;
  #UnitConvert, USE 1/35.31 TO CONVERT CFS TO CMS.
  ##############################################################################
  #Input Options
  #'csv': a comma-delimited file with table of daily streamflow in date-aligned rows, site-aligned columns
  #'rws': an R workspace with a dataframe of daily streamflow in date-aligned rows, site-aligned columns.
  #The input dataframe should have the name [ws].Rdata
  ###############################################################################
  #EXAMPLE FOR READING NWIS DATA FOR HUC 17 FOR WY1981-2010, 
  #USING LOW FLOW YEARS STARTING IN APRIL, 
  #PROCESSING SITES WITHOUT A SITE FILE
  #THAT HAVE A RECORD OF AT LEAST 10 YEARS WITH AT LEAST 360 DAYS
  #CONVERTING CFS TO CMS,
  #
  #INPUT FILE FORMAT
  #date, 12345678, 12345679, ....
  #1979-10-01, 100, 3.4, ...
  #1979-10-02, 98, 3.3, ...
  #
  #EXAMPLE OF R COMMAND:
  #flowmetrics('Qhuc2', 'flowmetrics', 'rws', 'oly', 'huc17.Rdata', 'oly.csv', 1991, 2010,  300, 5, 4, 1) 
  #
  #SITE FILE CAN BE USED TO CALCULATE METRICS AT SELECTED SITES OR AT THE SAME SITES FOR DIFFERENT PERIODS, 
  #THE FILE MUST HAVE 10 VALUES FOR EACH SITE/PERIOD WITH VALUES SEPARATED BY COMMAS:
  #     1) STATION ID,
  #     2) STARTING CALENDAR YEAR, 3) STARTING MONTH, 4) STARTING DAY, 
  #     5) ENDING CALENDAR YEAR, 6) ENDING MONTH, 7) ENDING DAY,
  #     8) MINIMUM NUMBER OF YEARS TO PROCESS A SITE,
  #     9) MINIMUM NUMBER OF DAYS PER YEAR TO PROCESS SITE,
  #     10) FIRST MONTH OF LOW FLOW YEARS (TYPICALLY 4 TO INDICATE APRIL)
  #THE STATION IDENTIFIER MUST MATCH THE IDENTIFIERS IN STREAMFLOW DATA
  # 
  #EXAMPLE OF SITE FILE INCLUDING HEADERS: 
  #STAID, START YEAR, START MONTH, START DAY, END YEAR, END MONTH, END DAY, MIN YEARS, MIN DAYS, MONTH FOR START OF LOW FLOW YEAR
  #12345678, 1980, 10, 1, 2015, 9, 30, 10, 360, 4
  #
  ##############################################################################
  #READ INPUT FILES AND CREATE ARRAYS OF DAILY STREAMFLOW FOR ALL STATIONS
  #INPUT CAN EITHER BE A TABLE OF DAILY STREAMFLOW OR AN R WORKSPACE
 q=read.table(file=paste(datadir, '/', InFile, sep=''), header=TRUE, sep = ',', check.names=FALSE)
  tmp.dates=q[,1]
  dimnames(q)[[1]]=q[,1]
  q=q[,-1]
  q[q=='NaN']=NA
  save(file=paste(datadir, '/', ws, '.Rdata', sep=''), 'q')
  
  dates=data.frame(tmp.dates, as.numeric(substr(tmp.dates, 1, 4)), as.numeric(substr(tmp.dates, 6, 7)), as.numeric(substr(tmp.dates, 9, 10)))
  dimnames(dates)[[2]]=c('Date','Year','Month','Day')
  dates$WY=dates$Year
  dates$WY[dates$Month>9]=dates$Year[dates$Month>9]+1
  
  num_days=dim(q)[[1]]
  num_stations=dim(q)[[2]]
  
  #############################################################
  #CREATE OR READ sites ARRAY WITH PROCESS CONTROL PARAMETERS
  #NOTE THAT SITES ARE GAGING STATIONS WHERE RECORDS ARE PROCESSED
  #IF THERE IS NO SITE FILE, RECORDS FOR ALL STATIONS ARE PROCESSED
  #THE SITE FILE CAN EXCLUDE STATIONS OR INCLUDE A STATION MULTIPLE TIMES WITH DIFFERENT PERIODS OF ANALYSIS
  #PROCESSED SITES ARE THE SITES THAT MEET THE MINIMUM NUMBER OF WATER YEARS FOR ANALYSIS 
  
  #SITE LIST FILE, USE VALUES SPECIFIED IN FILE
  sites=data.frame(read.table(file=SiteFile, header=TRUE, sep=',', colClasses=c('character', rep('numeric',9)), stringsAsFactors=FALSE))
  StartWY=min(sites$styr) # removed the +1 after styr, it used to say sites$stry+1
  EndWY=max(sites$endyr) 
  
  dimnames(sites)[[2]]=c('staid', 'styr', 'stmn', 'stdy', 'endyr', 'endmn', 'enddy', 'minyears', 'mindays','lfmon')
  sites$startdate=sites$styr*10000+sites$stmn*100+sites$stdy
  sites$enddate=sites$endyr*10000+sites$endmn*100+sites$enddy
  #sites$enddate2[sites$lfmon<10]=(sites$endyr[sites$lfmon<10]+1)*10000+sites$lfmon[sites$lfmon<10]*100 # REMOVED THIS LINE OF CODE because my end date is the date of species observation, not the following years low flow month
  num_sites=dim(sites)[[1]]
  
  ##########################################################################################
  #CENSOR FLOW RECORDS (q) FOR SELECTED SITES AND WATER YEARS
  #VECTOR cn HAS COLUMN NUMBERS FROM qin FOR EACH SITE
  #SITES ENTERED MULTIPLE TIMES WILL HAVE '.1', '.2', ... APPENDED TO COLUMN NAME IN ARRAY q
  cn=match(sites$staid, dimnames(q)[[2]])
  c('num_sites',num_sites)
  c('num with records',sum(is.na(cn)==0))
  cn[is.na(cn)]=length(q[1,])
  q=q[,cn]
  
  ###################################################
  #CREATE ARRAY dates FOR SPECIFIED WATER YEARS#
  dates$LowFlowYr=dates$Year
  if(LowFlowMonth>=10) {dates$LowFlowYr[dates$Month>=LowFlowMonth]=dates$WY[dates$Month>=LowFlowMonth]}
  if(LowFlowMonth<10) {dates$LowFlowYr[dates$Month<LowFlowMonth]=dates$Year[dates$Month<LowFlowMonth]-1} 
  
  #FILTER OUT DATES NOT IN WY OR LOW-FLOW YR FOR THE PERIOD OF ANALYSIS
  dates=dates[dates$WY %in% c(StartWY:EndWY) | dates$LowFlowYr %in% c(StartWY:EndWY),]
  
  dates$DateNum=dates$Year*10000+dates$Month*100+dates$Day
  num_days=length(dates$Year)
  
  #CENSOR DAILY STREAMFLOW FOR PERIOD OF ANALYSIS
  q=q[dimnames(q)[[1]] %in% dates$Date,]
  
  #CREATE ARRAY days THAT HAS 1 FOR EACH DAY WITH FLOW DATA 
  #IN THE PERIOD OF ANALYSIS AT A SITE
  #ALLOWS DIFFERENT PERIODS OF ANALYSIS AT DIFFERENT SITES
  #IF THEY ARE SPECIFIED IN THE SITE FILE
  days=mapply(function (x) (dates$DateNum>=sites$startdate[x])*(dates$DateNum<=sites$enddate[x]), 1:num_sites)
  days[days==0]=NA
  days[is.na(q)]=NA
  
  #############################################################################################
  #CREATE ARRAY wysinanalysis AND daysinwys THAT ARE USED TO SELECT SITES FOR PROCESSING
  #THAT MEET CRITERIA FOR MINIMUM NUMBER OF DAYS PER YEAR AND MINIMUM NUMBER OF YEARS
  daysinwy=apply(days, 2, function(x) tapply(x, dates$WY, sum, na.rm=TRUE))
  
  if(is.vector(daysinwy)) {daysinwy=rbind(daysinwy)
  dimnames(daysinwy)[[1]]=StartWY}
  
  daysinwy=daysinwy[dimnames(daysinwy)[[1]] %in% c(StartWY:EndWY), ]
  
  if(is.vector(daysinwy)) {daysinwy=rbind(daysinwy)}
  
  wysinanalysis=mapply(function(x) t(daysinwy[,x]>=sites$mindays[x]),1:num_sites)
  wysinanalysis[wysinanalysis==0]=NA
  if(is.vector(wysinanalysis)) {wysinanalysis=rbind(wysinanalysis)}
  
  sites$processed=apply(wysinanalysis, 2, sum, na.rm=TRUE)>=MinYears
  num_sites_processed=sum(sites$processed)
  wysinanalysis=wysinanalysis[,sites$processed==1]
  if(is.vector(wysinanalysis)) {wysinanalysis=rbind(wysinanalysis)}
  
  #FILTER ARRAY q FOR DAYS IN ANALYSIS
  q=q*days
  q=q[,sites$processed==1]
  days=days[,sites$processed==1]
  
  #########################################################
  #PROCESS HIGH/LOW FLOW EVENTS
  #low - NUMBER OF CONSECUTIVE (PRIOR) DAYS THAT STREAMFLOW WAS BELOW low_thresh 
  #high -NUMBER OF CONSECUTIVE (PRIOR) DAYS THAT STREAMFLOW WAS ABOVE high_thresh
  #nodisturb - NUMBER OF CONSECUTIVE (PRIOR) DAYS THAT STREAMFOW WAS BETWEEN low_thresh AND high_thresh
  
  #FUNCTION countdays COUNTS CONSECUTIVE DAYS MEETING CRITERIA 
  #USE TO CALCULATE MAXIMUM DURATION OF LOW, HIGH, NODISTURB, AND RECESSION
  #INPUT IS A DAILY SERIES OF LOGICAL VALUES (TRUE/FALSE) INDICATING THE CRITERIA ARE MET
  #OUTPUT IS AN VECTOR  OF DAILY NUMBER OF CONSECUTIVE PRIOR DAYS MEETING THE CRITERIA 
  countdays=function(input) {input[is.na(input)==1]=0;
  f1=cumsum(input)
  f2=c(0,(input[2:num_days]-input[1:(num_days-1)])==-1)*(f1) #VECTOR WITH CUMSUM FOR TRANSITIONAL DAYS THAT DON'T MEET CRITERIA
  f1-cummax(f2)} #VECTOR WITH CUMSUM FOR DAYS MEETING CRITERIA - CUMSUM FOR THE PREVIOUS PERIODS MEETING CRITERIA
  
  low=array(NA,c(num_days,num_sites_processed))
  high=array(NA,c(num_days,num_sites_processed))
  nodisturb=array(NA,c(num_days,num_sites_processed))
  
  #THRESHOLD FOR LOW FLOW EVENTS CAN BE ASSIGNED FOR EACH FLOW RECORD
  #low_thresh=apply(q,2,quantile,probs=0.1,na.rm=TRUE)
  #OR FOR EACH SITE USING THE FIRST FLOW RECORD FOR THE SITE
  low_thresh=apply(q[,match(sites$staid[sites$processed==1],sites$staid[sites$processed==1])],2, function(x) quantile(x, probs=0.1, na.rm=TRUE))
  sites$lowthresh=NA
  sites$lowthresh[sites$processed==1]=low_thresh
  
  #THRESHOLD FOR HIGH FLOW EVENTS CAN BE ASSIGNED FOR EACH FLOW RECORD
  #high_thresh=apply(q,2,quantile,probs=0.9,na.rm=TRUE)
  #OR FOR EACH SITE USING THE FIRST FLOW RECORD FOR THE SITE
  high_thresh=apply(q[,match(sites$staid[sites$processed==1],sites$staid[sites$processed==1])],2, function(x) quantile(x, probs=0.9, na.rm=TRUE))
  sites$highthresh=NA
  sites$highthresh[sites$processed==1]=high_thresh
  
  low=mapply(function(x) q[,x]<=low_thresh[x], 1:num_sites_processed)
  high=mapply(function(x) q[,x]>high_thresh[x], 1:num_sites_processed)
  nodisturb=((high+low)==0)
  low=apply(low,2,countdays)
  low[is.na(q)]=NA
  high=apply(high,2,countdays)
  high[is.na(q)]=NA
  nodisturb=apply(nodisturb,2,countdays)
  nodisturb[is.na(q)]=NA
  
  #####################################################
  #PERCENT DAILY CHANGE IN STREAMFLOW (qd - qd-1)/qd
  pdc=rbind(NA, (q[2:num_days,]-q[1:(num_days-1),])/q[1:(num_days-1),])
  pdc[q==0]=NA
  
  tmp1=(pdc<=0) #RECESSION DAYS: STREAMFLOW WAS HIGHER ON PREVIOUS DAY
  tmp1[is.na(tmp1)]=0
  recess=apply(tmp1, 2, countdays)
  
  #10-DAY RECESSION RATES
  r10d=rbind(NA, NA, NA, NA, NA, NA, NA, NA, NA, NA, (q[11:num_days,]-q[1:(num_days-10),])/q[1:(num_days-10),])
  r10d[r10d=='Inf']=NA
  
  tmp2=rbind(0, 0, 0, 0, 0, 0, 0, 0, 0, 0, tmp1[2:(num_days-9),]*tmp1[3:(num_days-8),]*tmp1[4:(num_days-7),]*tmp1[5:(num_days-6),]*tmp1[6:(num_days-5),]*tmp1[7:(num_days-4),]*tmp1[8:(num_days-3),]*tmp1[9:(num_days-2),]*tmp1[10:(num_days-1),]*tmp1[11:(num_days),])
  
  r10d[tmp2==0]=NA
  r10d4d=r10d*NA
  r10d4d[recess==14]=r10d[recess==14]
  
  ########################################################
  #PROCESS ANNUAL AND MONTHLY STATISTICS FOR EACH SITE
  #ARRAYS FOR ANNUAL STATISTICS 
  num_WYs=EndWY-StartWY+1
  annual_metrics=c('a_qmax', 'a_qmin', 'a_qmed', 'a_qmean', 'a_highnum', 'a_highdur', 'a_lownum', 'a_lowdur', 'a_nodisturb', 'a_noflow', 'a_r10d.5', 'a_r10d.9', 'a_recess', 'a_r10d4d','a_bfr', 'a_sfr')
  for (a in 1:length(annual_metrics)) {tmp=array(NA, c(num_WYs, num_sites_processed))
  dimnames(tmp) = list(c(StartWY:EndWY), sites$staid[sites$processed==1])
  assign(annual_metrics[a], tmp)}
  
  #ARRAYS FOR MONTHLY STATISTICS (YEARS x MONTH x SITES)
  #BUFFER FOR MONTHS IN YEARS WY-1 AND WY+1
  monthly_metrics=c('daysinmonth', 'monthinanalysis', 'month_qmax', 'month_qmin', 'month_qmed', 'month_qmean', 'month_highdur', 'month_lowdur', 'month_nodisturb', 'month_noflow')
  tmp=array(NA,c(num_WYs+2,12, num_sites_processed))
  dimnames(tmp)=list(c((StartWY-1):(EndWY+1)), c(1:12), sites$staid[sites$processed==1])
  for (a in 1:length(monthly_metrics)) {assign(monthly_metrics[a], tmp)}
  
  ############
  #SITE LOOP
  ############
  for (s in 1:num_sites_processed) {d=dates$WY %in% c(StartWY:EndWY)
  
  tmp=c(tapply(q[d,s],dates$WY[d],max,na.rm=TRUE))
  a_qmax[match(names(tmp), c(StartWY:EndWY)), s]=tmp
  
  tmp=c(tapply(q[d, s], dates$WY[d], median,na.rm=TRUE))
  a_qmed[match(names(tmp), c(StartWY:EndWY)), s]=tmp
  
  tmp=c(tapply(q[d, s], dates$WY[d], mean,na.rm=TRUE))
  a_qmean[match(names(tmp), c(StartWY:EndWY)), s]=tmp
  
  tmp=c(tapply(high[d, s], dates$WY[d], max,na.rm=TRUE))
  a_highdur[match(names(tmp), c(StartWY:EndWY)), s]=tmp
  
  tmp=c(tapply(high[d, s], dates$WY[d], function(x) sum(x==1,na.rm=TRUE)))
  a_highnum[match(names(tmp), c(StartWY:EndWY)), s]=tmp
  
  #DAILY RECESSION RATES
  tmp1=pdc[,s]
  tmp1[pdc[,s]>0]=NA
  
  tmp=c(tapply(tmp1[d], dates$WY[d], median, na.rm=TRUE))
  a_bfr[match(names(tmp), c(StartWY:EndWY)), s]=tmp
  
  tmp=c(tapply(tmp1[d], dates$WY[d], quantile, probs=c(0.1), na.rm=TRUE))
  a_sfr[match(names(tmp), c(StartWY:EndWY)), s]=tmp
  
  #MAXIMUM DURATION OF 'NO DISTURBANCE' PERIODS - WITH NEITHER HIGH NOR LOW FLOWS
  tmp=c(tapply(nodisturb[d,s], dates$WY[d], max,na.rm=TRUE))
  a_nodisturb[match(names(tmp), c(StartWY:EndWY)), s]=tmp
  
  #LOW FLOW STATS ARE SUMMARIZED BY LOW FLOW YEAR
  #LOW FLOW DURATION MAY BE LONGER THAN 365 DAYS
  d=dates$LowFlowYr %in% c(StartWY:EndWY)
  
  tmp=c(tapply(q[d, s], dates$LowFlowYr[d], min, na.rm=TRUE))
  a_qmin[match(names(tmp), c(StartWY:EndWY)), s]=tmp
  
  tmp=c(tapply(low[d, s],dates$LowFlowYr[d], max, na.rm=TRUE))
  a_lowdur[match(names(tmp), c(StartWY:EndWY)), s]=tmp
  
  tmp=c(tapply(q[d, s],dates$LowFlowYr[d], function(x) sum(x==0,na.rm=TRUE)))
  a_noflow[match(names(tmp), c(StartWY:EndWY)), s]=tmp
  
  tmp=c(tapply(low[d, s], dates$LowFlowYr[d], function(x) sum(x==1,na.rm=TRUE)))
  a_lownum[match(names(tmp), c(StartWY:EndWY)), s]=tmp
  
  #MAXIMUM LENGTH OF STREAMFLOW RECESSION
  tmp=c(tapply(recess[d, s], dates$LowFlowYr[d], max, na.rm=TRUE))
  a_recess[match(names(tmp), c(StartWY:EndWY)), s]=tmp
  
  #10-DAY RECESSION RATES FOR LOW FLOW YEARS
  tmp=c(tapply(r10d[d, s], dates$LowFlowYr[d], median, na.rm=TRUE))
  a_r10d.5[match(names(tmp), c(StartWY:EndWY)), s]=tmp
  
  tmp=c(tapply(r10d[d, s], dates$LowFlowYr[d], quantile,probs=c(0.9),na.rm=TRUE))
  a_r10d.9[match(names(tmp), c(StartWY:EndWY)), s]=tmp
  
  #10-DAY RECESSION RATES ONLY FOR PERIODS STARTING AFTER 4 DAYS OF RECESSION
  tmp=c(tapply(r10d4d[d, s], dates$LowFlowYr[d], median, na.rm=TRUE))
  a_r10d4d[match(names(tmp), c(StartWY:EndWY)), s]=tmp
  
  #########################################################################
  #PROCESS MONTHLY STATISTICS FOR MONTHS WITH COMPLETE RECORDS:
  #DIM 1 IS CALENDAR YEAR (StartWY-1 to EndWY); DIM 2 IS MONTH (Jan==1), DIM 3 IS SITE
  #TMP IS USED FOR INCOMPLETE RESULTS, 
  #WHICH ARE RE-INDEXED FOR ALL YEARS AND MONTHS IN FINAL ARRAYS
  #########################################################################
  tmp=tapply(is.na(q[,s])==0,list(dates$Year, dates$Month),sum,na.rm=TRUE)
  daysinmonth[match(as.numeric(dimnames(tmp)[[1]]), (StartWY-1):(EndWY+1)), as.numeric(dimnames(tmp)[[2]]),s]=tmp
  
  tmp=t(apply(daysinmonth[,,s],1,function(x) x>=c(31,28,31,30,31,30,31,31,30,31,30,31)))
  monthinanalysis[match(as.numeric(dimnames(tmp)[[1]]), (StartWY-1):(EndWY+1)), as.numeric(dimnames(tmp)[[2]]),s]=tmp
  
  tmp=tapply(q[,s],list(dates$Year,dates$Month),max,na.rm=TRUE)
  month_qmax[match(as.numeric(dimnames(tmp)[[1]]), (StartWY-1):(EndWY+1)), as.numeric(dimnames(tmp)[[2]]),s]=tmp
  
  tmp=tapply(q[,s],list(dates$Year,dates$Month),min,na.rm=TRUE)
  month_qmin[match(as.numeric(dimnames(tmp)[[1]]), (StartWY-1):(EndWY+1)), as.numeric(dimnames(tmp)[[2]]),s]=tmp
  
  tmp=tapply(q[,s],list(dates$Year,dates$Month),median,na.rm=TRUE)
  month_qmed[match(as.numeric(dimnames(tmp)[[1]]), (StartWY-1):(EndWY+1)), as.numeric(dimnames(tmp)[[2]]),s]=tmp
  
  tmp=tapply(q[,s],list(dates$Year,dates$Month),mean,na.rm=TRUE)
  month_qmean[match(as.numeric(dimnames(tmp)[[1]]), (StartWY-1):(EndWY+1)), as.numeric(dimnames(tmp)[[2]]),s]=tmp
  
  tmp=tapply(low[,s],list(dates$Year,dates$Month),function(x) sum(x>0,na.rm=TRUE))
  month_lowdur[match(as.numeric(dimnames(tmp)[[1]]), (StartWY-1):(EndWY+1)), as.numeric(dimnames(tmp)[[2]]),s]=tmp
  
  tmp=tapply(high[,s],list(dates$Year,dates$Month),function(x) sum(x>0,na.rm=TRUE))
  month_highdur[match(as.numeric(dimnames(tmp)[[1]]), (StartWY-1):(EndWY+1)), as.numeric(dimnames(tmp)[[2]]),s]=tmp
  
  tmp=tapply(q[,s],list(dates$Year,dates$Month),function(x) sum(x==0,na.rm=TRUE))
  month_noflow[match(as.numeric(dimnames(tmp)[[1]]), (StartWY-1):(EndWY+1)), as.numeric(dimnames(tmp)[[2]]),s]=tmp
  
  tmp=tapply(nodisturb[,s],list(dates$Year,dates$Month),max,na.rm=TRUE)
  month_nodisturb[match(as.numeric(dimnames(tmp)[[1]]), (StartWY-1):(EndWY+1)), as.numeric(dimnames(tmp)[[2]]),s]=tmp
  
  #SET MONTHLY STATISTICS TO NA FOR MONTHS WITHOUT COMPLETE FLOW DATA
  monthinanalysis[monthinanalysis==0]=NA
  monthinanalysis[monthinanalysis==0]=NA
  month_qmax[,,s]=month_qmax[,,s] * monthinanalysis[,,s]
  month_qmin[,,s]=month_qmin[,,s] * monthinanalysis[,,s]
  month_qmed[,,s]=month_qmed[,,s] * monthinanalysis[,,s]
  month_qmean[,,s]=month_qmean[,,s] * monthinanalysis[,,s]
  month_lowdur[,,s]=month_lowdur[,,s] * monthinanalysis[,,s]
  month_highdur[,,s]=month_highdur[,,s] * monthinanalysis[,,s]
  month_noflow[,,s]=month_noflow[,,s] * monthinanalysis[,,s]
  }   #CLOSE SITE LOOP
  
  #SET ANNUAL VALUES FOR YEARS NOT ANALYZED TO NA
  for (a in 1:length(annual_metrics)) {tmp=get(annual_metrics[a])
  assign(annual_metrics[a], tmp*wysinanalysis)
  #CONVERT INF/-INF VALUES TO NA	
  tmp[tmp==-Inf]=NA
  tmp[tmp==Inf]=NA}
  
  ##############################################
  #PROCESS PERIOD OF ANALYSIS METRICS (qsum)
  ##############################################
  qsum=as.data.frame(array(NA,c(num_sites_processed,39)))
  dimnames(qsum)[[2]]=c('Site', 'Years', 'StartWY', 'EndWY',
                        'Qmean','QmeanMEDIAN', 'QmeanIDR','Qmed','Qmax','QmaxIDR','HighNum','HighDur',
                        'Qmin', 'QminIDR','LowNum','LowDur','NoDisturb',
                        'Hydroperiod', 'FracYearsNoFlow', 'MedianNoFlowDays', 'RecessMaxLength',
                        'R10D.5', 'R10D.9', 'R10D4D', 'BFR', 'SFR',
                        'MaxMonth', 'MaxMonthQ', 'MinMonth','MinMonthQ',
                        'Q01','Q05', 'Q10', 'Q25', 'Q50', 'Q75', 'Q90', 'Q95','Q99')
  
  #SUMMARIZE OVER PERIOD OF ANALYSIS
  qsum$Site=sites[sites$processed,1]
  qsum$Years=apply(wysinanalysis,2, sum, na.rm=TRUE)
  qsum$StartWY=apply(wysinanalysis*(StartWY:EndWY),2, min, na.rm=TRUE)
  qsum$EndWY=apply(wysinanalysis*(StartWY:EndWY),2, max, na.rm=TRUE)
  qsum$Qmean=apply(q,2, mean, na.rm=TRUE)
  qsum$QmeanMEDIAN=apply(a_qmean, 2, median, na.rm=TRUE)
  tmp=apply(a_qmean, 2, quantile, probs=c(0.1,0.5,0.9), na.rm=TRUE)
  qsum$QmeanIDR=(tmp[3,]-tmp[1,])
  qsum$Qmed=apply(a_qmed, 2, median, na.rm=TRUE)
  qsum$R10D.5=apply(r10d, 2, median, na.rm=TRUE)
  qsum$R10D.9=apply(r10d, 2, quantile, p=0.9, na.rm=TRUE)
  qsum$R10D4D=apply(r10d4d, 2, median, na.rm=TRUE)
  qsum$BFR=apply(pdc, 2, function(x) quantile(x[x<=0],0.5, na.rm=TRUE))
  qsum$SFR=apply(pdc, 2, function(x) quantile(x[x<=0],0.1, na.rm=TRUE))
  qsum$Qmax=apply(a_qmax, 2, median, na.rm=TRUE)
  tmp=apply(a_qmax, 2, quantile, probs=c(0.1,0.5,0.9), na.rm=TRUE)
  qsum$QmaxIDR=(tmp[3,]-tmp[1,])
  qsum$HighNum=apply(a_highnum, 2, median, na.rm=TRUE)
  qsum$HighDur=apply(a_highdur, 2, median, na.rm=TRUE)
  qsum$Qmin=apply(a_qmin, 2, median, na.rm=TRUE)
  tmp=apply(a_qmin, 2, quantile, probs=c(0.1,0.5,0.9), na.rm=TRUE)
  qsum$QminIDR=(tmp[3,]-tmp[1,])
  qsum$LowNum=apply(a_lownum, 2, median, na.rm=TRUE)
  qsum$LowDur=apply(a_lowdur,2, median, na.rm=TRUE)
  qsum$NoDisturb=apply(a_nodisturb, 2, median, na.rm=TRUE)
  qsum$Hydroperiod=apply(q, 2, function(x) sum(x>0, na.rm=TRUE)/sum(is.na(x)==0))
  qsum$FracYearsNoFlow=apply(a_noflow, 2, function(x) sum(x>0, na.rm=TRUE))/qsum$Years
  qsum$MedianNoFlowDays=apply(a_noflow, 2, median, na.rm=TRUE)
  qsum$RecessMaxLength=apply(a_recess, 2, median, na.rm=TRUE)
  qsum[,31:39]=t(apply(q, 2, function(x) quantile(x, c(0.01, 0.05, 0.1, 0.25, 0.5, 0.75, 0.9, 0.95, 0.99), na.rm=TRUE)))
  
  ############################
  #SUMMARIZE MONTHLY STATS
  ############################
  # DIM 1 HAS SITES, DIM 2 HAS MONTHS (JAN-DEC), DIM 3 HAS STATS: 
  #1 - NUMBER OF YEARS WITH EACH MONTH
  #2 - FRACTION OF YEARS WITH LOW FLOW IN MONTH
  #3 - FRACTION OF YEARS WITH HIGH FLOW IN MONTH
  #4 - MEDIAN ANNUAL MEAN MONTHLY FLOW
  #5 - MEDIAN ANNUAL MEAN MONTHLY FLOW STANDARDIZED BY QMEAN
  #6 - MEDIAN ANNUAL MEDIAN MONTHLY FLOW
  #7 - MEDIAN ANNUAL DURATION OF NO DISTURBANCE IN MONTH
  
  monthsum=array(0,c(num_sites_processed,12,10))
  dimnames(monthsum)[[1]]=sites[sites$processed,1]
  dimnames(monthsum)[[2]]=c('Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec')
  dimnames(monthsum)[[3]]=c('NumYears','FracYrsLowFlow','FracYrsHighFlow','MedAnnualQmean','medAnnualQmeanSTD',
                            'MedAnnualQmed','MedAnnualNoDist','x1','x2','x3')
  
  monthsum[,,1]=t(apply(monthinanalysis,3,colSums,na.rm=TRUE))
  monthsum[,,2]=t(apply(month_lowdur,3,function(x) colSums(x>0,na.rm=TRUE)))
  monthsum[,,2]=monthsum[,,2]/monthsum[,,1]
  monthsum[,,3]=t(apply(month_highdur,3,function(x)  colSums(x>0,na.rm=TRUE)))
  monthsum[,,3]=monthsum[,,3]/monthsum[,,1]
  monthsum[,,4]=t(apply(cbind(1:num_sites_processed),1, function(x) apply(month_qmean[,,x],2,function(x) median(x, na.rm=TRUE))))
  monthsum[,,5]=monthsum[,,4]/qsum$Qmean
  monthsum[,,6]=t(apply(cbind(1:num_sites_processed),1, function(x) apply(month_qmed[,,x],2,function(x) median(x, na.rm=TRUE))))
  monthsum[,,7]=t(apply(cbind(1:num_sites_processed),1, function(x) apply(month_nodisturb[,,x],2,function(x) median(x, na.rm=TRUE))))
  
  qsum$MaxMonthQ=apply(monthsum[,,4],1,max, na.rm=TRUE)
  qsum$MinMonthQ=apply(monthsum[,,4],1,min, na.rm=TRUE)
  qsum$MaxMonth=apply(cbind(1:num_sites_processed),1,function(x) match(qsum$MaxMonthQ[x],monthsum[x,,4]))
  qsum$MinMonth=apply(cbind(1:num_sites_processed),1,function(x) match(qsum$MinMonthQ[x],monthsum[x,,4]))
  
  #CREATE WORKSPACE FOR METRICS
  ws=paste(ws,'_',StartWY,EndWY,sep='')
  WSFile=paste(ws,'_metrics.Rdata',sep='')
  
  index=cbind(c('InFile','SiteFile','WSFile', 'Number of Sites', 'Number of Sites Processed', 'UnitConvert','LowFlowMonth','MinDays','MinYears'),
              c(InFile,SiteFile,WSFile, num_sites, num_sites_processed, UnitConvert, LowFlowMonth, MinDays, MinYears))
  
  write.table(qsum,file=paste(outdir, '/', ws,'_qsum.csv',sep=''),sep=',',row.names=FALSE)
  
  save(file=paste(outdir, '/', WSFile, sep=''), list=c('a_bfr', 'a_highdur', 'a_highnum', 'a_lowdur', 'a_lownum', 'a_nodisturb', 'a_noflow', 'a_r10d.5', 'a_r10d.9', 'a_r10d4d', 'a_recess', 'a_qmax', 'a_qmean', 'a_qmed', 'a_qmin', 'a_sfr', 'countdays', 'dates', 'days', 'high','index', 'low', 'month_highdur', 'month_lowdur', 'month_nodisturb', 'month_noflow', 'month_qmax', 'month_qmean', 'month_qmed', 'month_qmin', 'monthinanalysis', 'monthsum', 'nodisturb', 'pdc', 'q','qsum', 'sites', 'wysinanalysis'))


#####################################################################################


dat3<-read.csv("C:/Users/JennyT/Documents/LitReview/RB4/WorkingData_3-16-18/MetricsForMarcus/3yr_test_19822014_qsum.csv")  
dat5<-read.csv("C:/Users/JennyT/Documents/LitReview/RB4/WorkingData_3-16-18/MetricsForMarcus/5yr_test_19822014_qsum.csv")  
dat10<-read.csv("C:/Users/JennyT/Documents/LitReview/RB4/WorkingData_3-16-18/MetricsForMarcus/10yr_test_19822014_qsum.csv")  
datall<- read.csv("C:/Users/JennyT/Documents/LitReview/RB4/WorkingData_3-16-18/MetricsForMarcus/all_test_19822014_qsum.csv")
 
#COMIDS
ID<-read_excel("C:/Users/JennyT/Documents/LitReview/RB4/WorkingData_3-16-18/MetricsForMarcus/COMID_File.xlsx") %>% 
  select(2) %>% 
  filter(watershedID != 51)

#3 year data
x3yr<-seq.Date(from = as.Date("1985/10/1"), to = as.Date("2014/9/30"), "quarter")

#5 year data
x5yr<-seq.Date(from = as.Date("1987/10/1"), to = as.Date("2014/9/30"), "quarter")

#10  year data
x10yr<-seq.Date(from = as.Date("1992/10/1"), to = as.Date("2014/9/30"), "quarter")

#make dataframe for each time frame
thre<- expand.grid(watershedID=ID$watershedID , date = x3yr)
thre<- thre[order(thre$watershedID),]

fiv<- expand.grid(watershedID=ID$watershedID , date = x5yr)
fiv<- fiv[order(fiv$watershedID),]

ten<- expand.grid(watershedID=ID$watershedID , date = x10yr)
ten<- ten[order(ten$watershedID),]


#get dates
dates3<- data.frame(
  "stid" = thre$COMID,
  "styr" = NA,
  "stmn" = as.numeric(month(thre$date)),
  "stdy" = as.numeric(day(thre$date)), 
  "endyr" = as.numeric(year(thre$date)), 
  "endmn" = as.numeric(month(thre$date)),
  "enddy" = as.numeric(day(thre$date)),
  "minyr" = 2,
  "mindays" = 200,
  "lowflowyear" = 4)
dates3<-mutate(dates3, styr=endyr-3)
dates3$stid<-as.numeric(dates3$stid)

dates5<- data.frame(
  "stid" = fiv$watershedID,
  "styr" = NA,
  "stmn" = as.numeric(month(fiv$date)),
  "stdy" = as.numeric(day(fiv$date)), 
  "endyr" = as.numeric(year(fiv$date)), 
  "endmn" = as.numeric(month(fiv$date)),
  "enddy" = as.numeric(day(fiv$date)),
  "minyr" = 2,
  "mindays" = 200,
  "lowflowyear" = 4)
dates5<-mutate(dates5, styr=endyr-5)
dates5$stid<-as.numeric(dates5$stid)

dates10<- data.frame(
  "stid" = ten$watershedID,
  "styr" = NA,
  "stmn" = as.numeric(month(ten$date)),
  "stdy" = as.numeric(day(ten$date)), 
  "endyr" = as.numeric(year(ten$date)), 
  "endmn" = as.numeric(month(ten$date)),
  "enddy" = as.numeric(day(ten$date)),
  "minyr" = 2,
  "mindays" = 200,
  "lowflowyear" = 4)
dates10<-mutate(dates10, styr=endyr-10)
dates10$stid<-as.numeric(dates10$stid)

dat10<- cbind(dates10,dat10)
dat5<- cbind(dates5,dat5)
dat3<- cbind(dates3,dat3)

ID<-read_excel("C:/Users/JennyT/Documents/LitReview/RB4/WorkingData_3-16-18/MetricsForMarcus/COMID_File.xlsx") %>% 
  select(1,2)
names(ID)[2]<-"stid"
  

dat10<-left_join(dat10, ID, by = "stid") %>% 
  select(-styr, -stmn, -stdy, -minyr, -mindays, -lowflowyear, -Site, -Years, -StartWY, -EndWY)
dat10$date<-mdy(paste(dat10$endmn, dat10$enddy, dat10$endyr, sep = "/"))
dat10<-dat10 %>% 
  select(-endmn, - enddy, -endyr)
names(dat10)[2:36]<-paste("x10", names(dat10)[2:36], sep = "_")
dat10<-dat10 %>% 
  select(1, 37, 38, 2:36)



dat5<-left_join(dat5, ID, by = "stid") %>% 
  select(-styr, -stmn, -stdy, -minyr, -mindays, -lowflowyear, -Site, -Years, -StartWY, -EndWY)
dat5$date<-mdy(paste(dat5$endmn, dat5$enddy, dat5$endyr, sep = "/"))
dat5<-dat5 %>% 
  select(-endmn, - enddy, -endyr)
names(dat5)[2:36]<-paste("x5", names(dat5)[2:36], sep = "_")
dat5<-dat5 %>% 
  select(1, 37, 38, 2:36)



dat3<-left_join(dat3, ID, by = "stid") %>% 
  select(-styr, -stmn, -stdy, -minyr, -mindays, -lowflowyear, -Site, -Years, -StartWY, -EndWY)
dat3$date<-mdy(paste(dat3$endmn, dat3$enddy, dat3$endyr, sep = "/"))
dat3<-dat3 %>% 
  select(-endmn, - enddy, -endyr)
names(dat3)[2:36]<-paste("x3", names(dat3)[2:36], sep = "_")
dat3<-dat3 %>% 
  select(1, 37, 38, 2:36)

datfinal<- left_join(dat3, dat5, by = c("COMID", "date"))
datfinal<- left_join(datfinal, dat10, by = c("COMID", "date"))


names(datall)[1]<-"stid"
datall<-left_join(datall, ID, by = "stid")
datall<-datall %>% 
  select(-Years, -StartWY, -EndWY, - stid)
names(datall)[1:35]<-paste("all", names(datall)[1:35], sep = "_")
datfinal<- left_join(datfinal, datall, by = "COMID")


write.csv(datfinal, file = "C:/Users/JennyT/Documents/LitReview/RB4/WorkingData_3-16-18/MetricsForMarcus/konrad_met.csv")
