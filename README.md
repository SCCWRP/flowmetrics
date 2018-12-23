# README

Metrics to model with StreamCat:

tenyrstorm, twoyrstorm, X5yrRBI, x3yr_SFR, allyr_R10D.5, x3yr_QmaxIDR, x5yr_RecessMaxLength, allyr_LowDur, x5yr_HighDur, x3yr_HighDur, x10yr_HighNum, allyr_MedianNoFlowDays, allyr_Qmax, allyr_Q99

## Data

* `comid_atts.RData` sf object of NHDplus flowlines and joined StreamCat data.  NHDplus flowlines were filtered to remove those in "unnatural settings" (by clustering of land use and presence of dam in watershed).  StreamCat data were filtered to remove those with near zero variance for the region (i.e., no predictive power) or they were redundant.  This file was created using source data outside of this project with `R/COMID clustering.R` from JT.
