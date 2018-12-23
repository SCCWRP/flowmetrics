# README

Metrics to model with StreamCat:

tenyr, twoyr, X5yrRBI, x3_SFR, all_R10D.5, x3_QmaxIDR, x5_RecessMaxLength, all_LowDur, x5_HighDur, x3_HighDur, x10_HighNum, all_MedianNoFlowDays, all_Qmax, all_Q99

## Data

* `comid_atts.RData` sf object of NHDplus flowlines and joined StreamCat data.  NHDplus flowlines were filtered to remove those in "unnatural settings" (by clustering of land use and presence of dam in watershed).  StreamCat data were filtered to remove those with near zero variance for the region (i.e., no predictive power) or they were redundant.  This file was created using source data outside of this project with `R/COMID clustering.R` from JT.

* `flowmet.RData` Flow metric data to model, includes data from above list taken from raw data file