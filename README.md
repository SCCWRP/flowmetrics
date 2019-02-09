# README

Model performance: [https://sccwrp.shinyapps.io/flowmetrics/modests.Rmd](https://sccwrp.shinyapps.io/flowmetrics/modests.Rmd)

## Data

* `comid_atts.RData` sf object of NHDplus flowlines and joined StreamCat data.  NHDplus flowlines were filtered to remove those in "unnatural settings" (by clustering of land use and presence of dam in watershed).  StreamCat data were filtered to remove those with near zero variance for the region (i.e., no predictive power) or they were redundant.  This file was created using source data outside of this project with `R/COMID clustering.R` from JT.

* `comid_pnts.RData` sf object of NHDplus flowline centroids, all COMIDs in `comid_atts.RData` and all others in watershed

* `flowmet.RData` Flow metric data to model, includes data from above list taken from raw data file

* `flowmetprf.RData` Model performance for estimated flow metrics
