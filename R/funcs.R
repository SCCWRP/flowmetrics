#' Get Konrad metrics, currently setup for using extracted precip data
#'
#' @param id vector of site IDs, used to filter flowin
#' @param flowin actual flow data, for qin_create
#' @param dtstrt starting date, for sitefile_create, corrected by subnm
#' @param dtend ending date, for sitefile_create, can also include multiple dates
#' @param subnm metric types as 3, 5, 10, or all years
#' @param inps logical to return q and sites objects for additional metrics
konradfun <- function(id, flowin, dtstrt = '1982/10/1', dtend = '2014/9/30', subnm = c('x3', 'x5', 'x10', 'all'), inps = FALSE){

  # check arg  
  subnm <- match.arg(subnm)

  if(subnm == 'all' & any(duplicated(id)))
    stop('no duplicated id values of subnm = "all"')

  # qin_create
  qin_create <- flowin %>% 
    filter(COMID %in% id) %>%
    spread(COMID, dly_prp) %>% 
    dplyr::rename(Date = date) %>% 
    data.frame
  names(qin_create) <- gsub('^X', '', names(qin_create))
  
  # create site file, all
  sitefile_create <- data.frame(
    "stid" = as.numeric(names(qin_create)[-1]),
    "styr" = as.numeric(format(qin_create[366,1], "%Y")),
    "stmn" = as.numeric(format(qin_create[1,1], "%m")),
    "stdy" = as.numeric(format(qin_create[1,1], "%d")),
    "endyr" = as.numeric(format(qin_create[nrow(qin_create),1], "%Y")),
    "endmn" = as.numeric(format(qin_create[nrow(qin_create),1], "%m")),
    "enddy" = as.numeric(format(qin_create[nrow(qin_create),1], "%d")),
    "mindays" = 200,
    "minyr" = 2,
    "lowflowyear" = 4)
  
  # sitefile create if not all
  if(subnm != 'all'){
    
    # correction factor for start year
    yrfct <- gsub('^x', '', subnm) %>% 
      as.numeric
    
    # for generic site file
    if(length(dtend) == 1){
      
      dtstrt <- as.Date(dtstrt) %m+% years(yrfct)
      dtend <- as.Date(dtend)

      # expand grid
      xyr<-seq.Date(from = dtstrt, to = dtend, "quarter") %>% 
        crossing(COMID = id, date = .)
   
    # otherwise use input
    } else {

      # need to filter ID, dtend by those in limits of flowin
      mindt <- min(flowin$date) %m+% years(yrfct)
      ID <- ID[dtend >= mindt]
      dtend <- dtend[dtend >= mindt]
      
      xyr <- tibble(
        COMID = ID, date = dtend
      ) 
      
    }
    
    #Create "sitefile"
    sitefile_create<- data.frame(
      "stid" = xyr$COMID,
      "styr" = NA,
      "stmn" = as.numeric(month(xyr$date)),
      "stdy" = as.numeric(day(xyr$date)),
      "endyr" = as.numeric(year(xyr$date)),
      "endmn" = as.numeric(month(xyr$date)),
      "enddy" = as.numeric(day(xyr$date)),
      "minyr" = 2,
      "mindays" = 200,
      "lowflowyear" = 4)
    sitefile_create<-mutate(sitefile_create, styr=endyr - yrfct)  
    sitefile_create$stid<-as.numeric(sitefile_create$stid)

  }
  
  #####################Begin Konrad Script###################################

  #flowmetrics
  #An R function used to calculate streamflow metrics from a table of daily streamflow.
  #
  #############################################################################
  #ARGUMENTS:
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
  
  LowFlowMonth<-4
  MinDays <-200
  MinYears <- 2
  UnitConvert<-1/35.31
  
  ##############################################################################
  #READ INPUT FILES AND CREATE ARRAYS OF DAILY STREAMFLOW FOR ALL STATIONS
  #INPUT CAN EITHER BE A TABLE OF DAILY STREAMFLOW OR AN R WORKSPACE
  q = qin_create
  tmp.dates=q[,1]
  dimnames(q)[[1]]=q[,1]
  q=q[,-1]
  q[q=='NaN']=NA
  
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
  sites=sitefile_create
  StartWY=min(sites$styr) # removed the +1 after styr, it used to say sites$stry+1
  EndWY=max(sites$endyr) 
  
  dimnames(sites)[[2]]=c('staid', 'styr', 'stmn', 'stdy', 'endyr', 'endmn', 'enddy', 'minyears', 'mindays','lfmon')
  sites$startdate=sites$styr*10000+sites$stmn*100+sites$stdy
  sites$enddate=sites$endyr*10000+sites$endmn*100+sites$enddy
  #sites$enddate2[sites$lfmon<10]=(sites$endyr[sites$lfmon<10]+1)*10000+sites$lfmon[sites$lfmon<10]*100 # REMOVED THIS LINE OF CODE because my end date is the date of species observation, not the following years low flow month
  num_sites=dim(sites)[[1]]
  
  # exit for addl metrics if true
  if(inps)
    return(list(q = q, sites = sites))
  
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
  q=q[dimnames(q)[[1]] %in% as.character(dates$Date),]
  
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
  
  cat(subnm, s, 'of', num_sites_processed, '\n')
  
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
  
  # combine output
  out <- cbind(sitefile_create, qsum) %>% 
    dplyr::select(-styr, -stmn, -stdy, -minyr, -mindays, -lowflowyear, -Site, -Years, -StartWY, -EndWY) %>% 
    unite('date', endmn, enddy, endyr, sep = '/') %>% 
    mutate(
      date = mdy(date), 
      ktype = subnm
    ) %>% 
    gather('met', 'val', -stid, -date, -ktype)
  
  return(out)
  
}

######
#' monthly flow estimates
#' 
#' @param q formatted flow data as output from konrad_fun with input as true
#' @param sites formatted flow data as output from konrad_fun with input as true 
mofl_fun <- function(q, sites){

  # get sites that have flow
  sites <- sites %>% 
    filter(staid %in% names(q))
 
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
    gather("SITE","flow", -date, -month, -year)
  
  # calculating monthly mean
  monthly_mean <- newdat %>% 
    group_by(SITE,year, month) %>% 
    summarize(flow = mean(flow, na.rm = T))
  
  out <- NULL  
  for (i in 1:nrow(sites)){
    
    # cat(i, '\t')
    
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
      dplyr::select(month, flow) %>% 
      spread(month, flow)
    
    out <- rbind(out, flows)
    
  }
  
  out <- cbind(sites, out)
  
  return(out)
  
}

######
#' richard baker index, by year groups
#'
#' @param q formatted flow data as output from konrad_fun with input as true
#' @param sites formatted flow data as output from konrad_fun with input as true
rbiyrs_fun <- function(q, sites){
  
  # richards baker index past 3, 5,10, and all years
  
  # get sites with flow data
  sites <- sites %>% 
    filter(staid %in% names(q))
  
  test<-q
  test$date<-row.names(q)
  test$month<-month(test$date)
  test$year<-year(test$date)
  
  sites<-sites[,c(1,5,6,7)]
  sites<-unique(sites)
  names(sites)[1]<-"SITE"
  
  #make flow in long format and then group by location, year, month
  
  newdat<-test %>% 
    gather("SITE","flow", -date, -month, -year)
  
  yrs <- c(3, 5, 10)
  
  final <- NULL  
  for (i in 1:nrow(sites)){
    
    # cat(i, '\t')
    
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
  
  out <- cbind(sites, final) %>% 
    rename(
      `3yrRBI` = `1`,
      `5yrRBI` = `2`,
      `10yrRBI` = `3`
    )
  
  return(out)
  
}

######
#' Richard Baker index, all years
#' 
#' @param q formatted flow data as output from konrad_fun with input as true
#' @param sites formatted flow data as output from konrad_fun with input as true
rbiall_fun <- function(q, sites){
  
  # richards baker index all years
  
  # get sites with flow data
  sites <- sites %>% 
    filter(staid %in% names(q))
  
  test<-q
  test$date<-row.names(q)
  test$month<-month(test$date)
  test$year<-year(test$date)
  
  sites<-sites[,c(1,5,6,7)]
  sites<-unique(sites)
  names(sites)[1]<-"SITE"
  
  #not sure if this is right because stable streams were given high values and vice versa..
  out <- test %>% 
    gather("SITE","flow", -date, -month, -year) %>% 
    group_by(SITE) %>% 
    summarize(rbi = sum(abs(diff(flow)))/sum(flow)) %>% 
    ungroup %>% 
    mutate(SITE = as.numeric(SITE))
  
  return(out)
  
}

######
#' get storm peaks
#'
#' @param q formatted flow data as output from konrad_fun with input as true
#' @param sites formatted flow data as output from konrad_fun with input as true
strm_fun <- function(q, sites){

  # filter sites by those with flow
  sites <- sites %>% 
    filter(staid %in% names(q))
  
  test<-q
  test$date<-row.names(q)
  test$month<-month(test$date)
  test$year<-year(test$date)
  
  sites<-sites[,c(1,5,6,7)]
  sites<-unique(sites)
  names(sites)[1]<-"SITE"
  
  
  #make flow in long format and then group by location, year, month
  
  newdat<-test %>% 
    gather("SITE","flow", -date, -month, -year)
  
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
        # tenyr <- mean(pks[1:3])
        fivyr <- mean(pks[4:9])
        twoyr <- mean(pks[10:24])
        
        # format output
        # out <- data.frame(tenyr = tenyr, fivyr = fivyr, twoyr = twoyr)
        out <- data.frame(fivyr = fivyr, twoyr = twoyr)
        
        return(out)
        
      })
    ) %>% 
    dplyr::select(-data) %>% 
    unnest %>% 
    gather('strm', 'mags', -SITE) %>% 
    mutate(SITE = as.numeric(SITE))
  
  toiter <- sites %>% 
    left_join(strmpks, by = 'SITE')
  
  final <- NULL
  
  for(i in 1:nrow(toiter)){
    
    # cat(i, '\n')
    
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
    
    # cat('\t', as.character(rw), '\n')
    # cat('\t', as.character(flows[strmind, ]),'\n')
    # cat('\t', dys, '\n')
    
    # append to output
    final <- c(final, dys)
    
  }
  
  out <- toiter %>% 
    mutate(
      cnts = final
    ) %>% 
    dplyr::select(-mags) %>% 
    spread(strm, cnts)
  
  return(out)
  
}

######
#' Process and combine additional metrics output
#' 
#' @param id vector of site IDs, used to filter flowin
#' @param flowin actual flow data, for qin_create
#' @param dtstrt starting date, for sitefile_create, corrected by subnm
#' @param dtend ending date, for sitefile_create, can be multiple
#' @param subnm metric types as 3, 5, 10, or all years
addlmet_fun <- function(id, flowin, dtstrt = '1982/10/1', dtend = '2014/9/30', subnm = c('x3', 'x5', 'x10', 'all')){
  
  # get qin, sitefile from konrad
  subnm <- match.arg(subnm)
  inps <- konradfun(id = id, flowin = flowin, dtstrt = dtstrt, dtend = dtend, subnm = subnm, inps = T)
  q <- inps$q
  sites <- inps$sites

  ##
  # prcoess metrics

  mofl <- mofl_fun(q, sites)
  rbiyrs <- rbiyrs_fun(q, sites)
  rbiall <- rbiall_fun(q, sites)
  strm <- strm_fun(q, sites)

  # combine output
  out <- strm %>% 
    full_join(rbiyrs, by = c("SITE", "endyr", "endmn", "enddy")) %>% 
    full_join(rbiall, by = "SITE") %>% 
    full_join(mofl, by = c("SITE", "endyr", "endmn")) %>% 
    unite('date', endmn, enddy, endyr, sep = '/') %>% 
    mutate(
      date = mdy(date),
      ktype = subnm
      ) %>% 
    rename(stid = SITE) %>% 
    arrange(stid, date) %>% 
    gather('met', 'val', -stid, -date, -ktype)
  
  return(out)
  
}

######
#' predict flow metrics from precip metrics and static sreamcat predictors
#' done by refitting rf model for each flow metric using top predictors (static and precip metrics)
#' top predictors ided from flow flow metric performance estimates from rf mods calculated separately
#' then predictions are made for each flow metric from extracted precip metrics for location, dates
#' 
#' @param obsflowmet observed flow metric for training rf model 
#' @param trnprecipmet observed precip metrics for training rf model
#' @param flowmetprf estimated performance and predicors for rf models for each flow metric
#' @param prdprecipmet input precip metrics at locations and dates for predicting flow metrics from rf models
#' @param comid_attsall input static streamcat data at COMIDs for predicting flow metrics from rf models
#' 
#' @details Models are fit and metrics predicted, requires setup of parallel backend
#' 
flowmetprd_fun <- function(obsflowmet, trnprecipmet, flowmetprf, prdprecipmet, comid_attsall){

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
  
  # best model for each metric, july only
  # based on avg rsq across folds for validation sets only
  bstmods <- flowmetprf %>% 
    filter(mo %in% 7) %>% 
    filter(set %in% 'val') %>% 
    group_by(var, modtyp) %>% 
    summarise(rsqr = mean(rsqr, na.rm = T)) %>% 
    filter(rsqr == max(rsqr)) %>% 
    dplyr::select(-rsqr) %>% 
    ungroup
  
  # join best mod type with imp variables
  impvarsall <- impvarsall %>% 
    left_join(bstmods, by = 'var')
  
  # static predictors
  static <- comid_attsall %>%
    st_set_geometry(NULL)
  
  # setup data to model
  tomod <- obsflowmet %>%
    dplyr::select(-tenyr) %>% # do not model tenyr
    mutate(
      mo = month(date)
    ) %>%
    group_by(mo) %>% 
    mutate(folds = sample(1:5, length(mo), replace = T)) %>%
    ungroup %>%
    filter(mo %in% 7) %>% 
    gather('var', 'val', -watershedID, -COMID, -date, -mo, -folds) %>%
    left_join(comid_attsall, by = 'COMID') %>%
    left_join(trnprecipmet, by = c('COMID', 'date')) %>%
    group_by(var, mo) %>%
    nest
  
  # prediction data from the baseline precipmetrics
  # join with static comid predictors
  preddat <- prdprecipmet %>% 
    left_join(static, by = 'COMID')
  
  # for log
  strt <- Sys.time()
  
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
    
    # important predictors for the var
    impvars <- impvarsall %>% 
      filter(var %in% !!var) %>% 
      pull(impvars) %>% 
      unlist
    
    # get the best performing model for the var (all or top preds)
    modtyp <- impvarsall %>% 
      filter(var %in% !!var) %>% 
      pull(modtyp) %>% 
      unlist
    
    # use only one fold
    fld <- 1
    
    # calibration data
    calset <- data %>%
      filter(!folds %in% fld)
    
    # model formula, from top predictors or all 
    if(modtyp == 'prd')
      frmimp <- names(calset)[!names(calset) %in% c('watershedID', 'COMID', 'date', 'yr', 'var', 'folds', 'val', 'tenyr', 'geometry')] %>% 
        paste(collapse = '+') %>% 
        paste0('val~', .) %>% 
        formula
    if(modtyp == 'prdimp')
      frmimp <- impvars %>%
        paste(collapse = '+') %>%
        paste0('val~', .) %>%
        formula

    # create model, from top ten
    modimp <- randomForest(frmimp, data = calset, ntree = 500, importance = TRUE, na.action = na.omit, keep.inbag = TRUE)
    
    # predictions, all comid attributes
    prdimp <- predict(modimp, newdata = preddat, predict.all = T)
    bnds <- apply(prdimp$individual, 1, function(x) quantile(x, probs = c(0.1, 0.9), na.rm = T))
    cv <- apply(prdimp$individual, 1, function(x) sd(x, na.rm = T)/mean(x, na.rm = T))
    
    # combine output, estimates, lo, hi, cv, dataset
    out <- tibble(
      COMID = preddat$COMID,
      dtsl = preddat$dtsl, 
      est = prdimp$aggregate,
      lov= bnds[1, ],
      hiv= bnds[2, ],
      cv = cv
      ) %>%
      mutate(set = ifelse(COMID %in% calset$COMID, 'cal', 'notcal'))
    
    # this is for obs bio, needs actual date column to output
    if('date' %in% names(preddat))
      out <- bind_cols(out, date = preddat$date)
    
    return(out)
    
  }
  
  # final output
  flowmetest <- tomod %>%
    dplyr::select(-data) %>%
    bind_cols(., enframe(modsest)) %>%
    dplyr::select(-name) %>%
    unnest(value) 

  return(flowmetest)
  
}

######
#' extract simulated precipitation data at lat/lon points from input file list
#' extraction file list is where h5 files live
#' 
#' @param fls input file chr vector for location of h5 files, full paths
#' @param comid_pnts sf point object of locations to extract daily precip estimates
#' 
#' @details can be run in parallel
#' 
simextract_fun <- function(fls, comid_pnts){
  
  # geographic projection
  decprj <- "+proj=longlat +ellps=WGS84 +datum=WGS84 +no_defs"
  
  # reproject points for extraction
  comid_sel <- comid_pnts %>% 
    st_transform(decprj)

  # lat and lon bounding box for h5 files
  lats <- c(33.5, 35.01101)
  lons <- c(-119.70, -117.37)

  # for log
  strt<-Sys.time()
  
  # process
  res <- foreach(i = seq_along(fls), .packages = c('tidyverse', 'sf', 'raster', 'h5')) %dopar% {
    
    # log
    sink('log.txt')
    cat(i, 'of', length(fls), '\n')
    print(Sys.time()-strt)
    sink()
    
    # select one h5 file, open connection
    h5flcon <- h5file(name = fls[i], mode = "r")

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
      raster::extract(comid_sel) %>% 
      rowSums(na.rm = T) %>% 
      tibble(COMID = comid_pnts$COMID, dly_prp = .)
    
    # close the connection
    h5close(h5flcon)
    
    return(dly)
    
  }
  
  # combine output to save
  names(res) <- basename(fls)
  ext <- res %>% 
    enframe %>% 
    unnest %>% 
    mutate(
      date = gsub('^.*\\_([0-9]+)\\.h5$', '\\1', name),
      date = lubridate::ymd(date)
    ) %>% 
    dplyr::select(date, COMID, dly_prp)
  
  return(ext)
  
}

#' konrad metric estimates but in batch
#'
#' @ ID numeric vector of COMID 
#' @ dtsls dates where metrics are estimated
#' @ precipext extracted daily precipitation time series at each COMID
#'   
konrad_funbtch <- function(ID, dtsls, precipext){
  
  # log
  strt <- Sys.time()

  # estimate konrad
  res <- foreach(i = c('x3', 'x5', 'x10', 'all'), .packages = c('tidyverse', 'lubridate'), .export = c('ID', 'dtsls', 'precipext')) %dopar% {
    
    source('R/funcs.R')
    
    IDin <- ID
    if(i == 'all')
      IDin <- unique(IDin)

    out <- konradfun(id = IDin, flowin = precipext, dtend = dtsls, subnm = i)
    return(out)
    
  }
  
  print(Sys.time() - strt)
  
  kradprecipmet <- do.call('rbind', res) %>%
    rename(COMID = stid)
  
  return(kradprecipmet)
  
}

#' additional metric estimates but in batch
#'
#' @ ID numeric vector of COMID 
#' @ dtsls dates where metrics are estimated
#' @ precipext extracted daily precipitation time series at each COMID
#' 
addlmet_funbtch <- function(ID, dtsls, precipext){
  
  ##
  # estimate additional metrics, not Konrad
  
  # log
  strt <- Sys.time()
  
  #prep site file data, ~2 hrs
  res <- foreach(i = c('x3', 'x5', 'x10', 'all'), .packages = c('tidyverse', 'lubridate')) %dopar% {
    
    if(i == 'all')
      ID <- unique(ID)
    source('R/funcs.R')
    out <- addlmet_fun(id = ID, flowin = precipext, dtend = dtsls, subnm = i)
    return(out)
    
  }
  
  print(Sys.time() - strt)
  
  addlprecipmet <- do.call('rbind', res) %>%
    rename(COMID = stid)
  
  return(addlprecipmet)
  
}

#' Combine metric output from konrad_funbtch, addlmet_funbtch, only for future climate change predictions
#' 
#' @param kradprecipmet konrad ouput from konrad_funbtch
#' @param addlprecipmet additional metric output from addlmet_funbtch
#' @param enddt the end date in the time series
#' 
precipcmb_fun <- function(kradprecipmet, addlprecipmet){
  
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
          # filter(!date %in% as.Date(enddt)) %>%  # placeholder date from all metrics
          # na.omit %>%
          unique %>%
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
  
  return(precipmet)
  
}
