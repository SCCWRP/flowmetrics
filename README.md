# README

Application for exloring model performance of stream flow metrics for the LA/Ventura county region

Model performance: [https://sccwrp.shinyapps.io/flowmetrics/modests.Rmd](https://sccwrp.shinyapps.io/flowmetrics/modests.Rmd)

## Data

* `biodat.RData` observed biological data, locations, dates, and species p/a, from JT

* `bioflowmetest.RData` estimated flow metrics from rf models where biology was observed in `biodat.RData`

* `bioprecipmet.RData` calculated precipitation metrics where biology was observed, used to predict `bioflowmetest.RData`

* `bsext.RData` daily precipitation time series for baseline years (all) at all COMIDs

* `bsflowmetest.RData` estimated flow metrics from rf models for selected baseline years 1993 (wet), 2010 (moderate), 2014 (dry), all COMID

* `bsgrdext.RData` Extracted baseline precip data for Rosi

* `bsprecipmet.RData` calculated baseline precipitation metrics for selected years at all COMIDs, used to predict `bsflowmetest.RData`

* `CanESM2ext.RData` daily precipitation time series for future years at all COMIDs under CanESM2 scenario

* `canesm2flowmetdt1.RData` estimated flow metrics from rf models at all COMID for 2040 under CanESM2 scenario 

* `canesm2flowmetdt2.RData` estimated flow metrics from rf models at all COMID for 2100 under CanESM2 scenario 

* `canesm2precdt1.RData` calculated precipitation metrics at all COMID for 2040 under CanESM2 scenario 

* `canesm2precdt2.RData` calculated precipitation metrics at all COMID for 2100 under CanESM2 scenario

* `CCSM4ext.RData` daily precipitation time series for future years at all COMIDs under CCSM4 scenario

* `ccsm4flowmetdt1.RData` estimated flow metrics from rf models at all COMID for 2040 under CCSM4 scenario 

* `ccsm4flowmetdt2.RData` estimated flow metrics from rf models at all COMID for 2090, 2095, 2100 under CCSM4 scenario 

* `ccsm4precdt1.RData` calculated precipitation metrics at all COMID for 2040 under CCSM4 scenario 

* `ccsm4precdt2.RData` calculated precipitation metrics at all COMID for 2090, 2095, 2100 under CCSM4 scenario

* `comid_atts.RData` sf object of NHDplus flowlines and joined StreamCat data.  NHDplus flowlines were filtered to remove those in "unnatural settings" (by clustering of land use and presence of dam in watershed).  StreamCat data were filtered to remove those with near zero variance for the region (i.e., no predictive power) or they were redundant.  This file was created using source data outside of this project with `R/COMID clustering.R` from JT.

* `comid_attsall.RData` same as `comid_atts.RData` but includes all COMIDs in the study region

* `comid_pnts.RData` sf object of NHDplus flowline centroids, all COMIDs in `comid_atts.RData` and all others in watershed

* `flowmet.RData` observed flow metrics used to train rf models, taken from raw data file

* `flowmetdt2.RData` calculated precipitation metrics at all COMID for 2090, 2095, 2100, averaged across all climate models

* `flowmetprf.RData` Model performance for estimated flow metrics in `flowmet.RData`, nested tibble used as lookup for model fitting

* `MIROC5ext.RData` daily precipitation time series for future years at all COMIDs under MIROC5 scenario

* `miroc5flowmetdt1.RData` estimated flow metrics from rf models at all COMID for 2040 under MIROC5 scenario 

* `miroc5flowmetdt2.RData` estimated flow metrics from rf models at all COMID for 2090, 2095, 2100 under MIROC5 scenario 

* `MIROC5precdt1.RData` calculated precipitation metrics at all COMID for 2040 under MIROC5 scenario 

* `MIROC5precdt2.RData` calculated precipitation metrics at all COMID for 2100 under MIROC5 scenario

* `precipmet.RData` calculated precipitation metrics used to train rf models to predict observed flow metrics in `flowmetprf.RData`
