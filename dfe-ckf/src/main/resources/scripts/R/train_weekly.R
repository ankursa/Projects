# This version is the same as main except I attempt to rewrite everything using dsetools
# and weekly version
rm(list=ls())
require(data.table)
require(methods)
require(plyr)
require(dplyr)
v<-View


stdin <- file("stdin", "r")

args <- commandArgs(trailingOnly = TRUE)
hive_db_dir <- args[1]
outputLoc <- args[2]

## Define global variables 
baseunitsWindow <- 8
# In testing, adding tpc resulted in overforecast in holdout due to multicollinearity with price.  Just let price
# take care of it
#promoVars = c("circular", "tpc")
numWeeksToForecast = 52
source("createWeeklyModelData.R")
source("createSimpleForecasts.R")
source("util.R")
options(scipen = 10)
options(stringsAsFactors = FALSE)


main <- function(hive_db_dir, key){

  # Change outputLoc to add key to the table
  outputLoc <- paste0(outputLoc)
  
  namesList <- getInputNames(hive_db_dir, key)
  write.table(namesList$subgroups, stderr())
  write.table(namesList$sales, stderr())
  
  subGroups <- readSubgroups(namesList, key)
  
  salesData <- tryCatch({
    
    salesFile <- file.path(paste0("sales_", key, ".txt")) 
    salesDataLoc <- system(paste0("hadoop fs -getmerge ", namesList$sales, " ", salesFile))
    if(FALSE){
      #DEBUG
      salesFile <- "sales_new_fake.txt"
      subgroupsFile <- "subgroups_new_fake.txt"
      subGroups <- fread(subgroupsFile, stringsAsFactors = FALSE, header = FALSE, na.strings = c("\\N", "NA", "NULL"), sep = "|")
      names(subGroups) <- c("mdse_item_i", "mdse_sbcl_ref_i", "mdse_sbcl_n")
      subGroups <- prepSubGroups(subGroups)
      outputLoc <- getwd()
      key <- "new_fake"
    }
    origSalesData <- fread(salesFile, stringsAsFactors = FALSE, header = FALSE, na.strings = c("\\N", "NA", "NULL"), sep = "|")
    names(origSalesData) <- c(
      "mdse_item_i",
      "holiday_array",
      "revenue",  #sls_retl_a
      "retl_a",
      "sls_unit_q",
      "sales_storecount", #n_stores
      "fcg_q",
      "boh_q",
      "clearance_pct",
      "max_promo_daynr",
      "max_promo_pctoff",
      "max_promo_dollaroff",
      "max_external_pctoff",
      "max_external_dollaroff",
      "circular", #circular
      "circular_flag_count",
      "tpc",  # tpc_flag
      "tpc_flag_count",
      "clearance_flag",
      "clearance_flag_count",
      "dollar_off_flag",
      "dollar_off_flag_coun",
      "qty_for_dollar_flag",
      "qty_for_dollar_flag_",
      "free_product_flag",
      "free_product_flag_co",
      "pct_off_flag",
      "pct_off_flag_count",
      "giftcard_flag",
      "giftcard_flag_count",
      "external_promo_flag",
      "external_promo_flag_count",
      "pictured_flag_count",
      "pictured_flag",
      "min_pagenumber",
      "stockout_flag_count",
      "stockout_flag",
      "wk_begin_date",# week_start_date
      "cpn_f_sum", #cpn_f
      "cpn_f_count",
      "cpn_f", #wt_cpn_f
      "pmtn_item_f_sum",
      "pmtn_item_f_count",
      "pmtn_item_f", #wt
      "tpr_f_sum",
      "tpr_f_count",
      "tpr_f",   # wt
      "prc_guar_chit_f_sum",
      "prc_guar_chit_f_count",
      "prc_guar_chit_f",
      "circ_f_sum",
      "circ_f_count",
      "circ_f",
      "n_stores_asmt",
      "n_stores_sales",
      "own_retl_a", #med_own_retl_a
      "reg_retl_a",  #med_reg_retl_a
      "med_customer_paid_price",
      "wt_own_retl_a",
      "wt_reg_retl_a",
      "avg_ext_sls_prc_unit", #wt_customer_paid_price
      "wk_end_date" # week_end_date
    )
    origSalesData$wk_begin_date <- as.Date(origSalesData$wk_begin_date)
    origSalesData$wk_end_date <- as.Date(origSalesData$wk_end_date)
    origSalesData$sls_unit_q <- as.numeric(origSalesData$sls_unit_q)
    origSalesData$revenue <- as.numeric(origSalesData$revenue)
    origSalesData$sls_retl_a <- with(origSalesData, revenue/sls_unit_q)

    # Need to add check to throw out records with bad prices everywhere.  I won't be able to impute
    # if there are no useable records
    origSalesData$bad <- with(origSalesData, ifelse(own_retl_a <= 0.10 & reg_retl_a <= 0.1 & med_customer_paid_price <= 0.1, 1, 0))
    origSalesData <- data.table(origSalesData)
    counts <- origSalesData[, .(count = .N, bad = sum(bad)), by = c("mdse_item_i")]    
    goodCounts <- subset(counts, bad != count)    
    if(nrow(goodCounts) == 0){
      errorMsg <- paste0("No data due to bad prices ", key)
      show(errorMsg)
      warnFile <- paste0("warn_sales_", key, ".txt")
      write.csv(errorMsg, warnFile, row.names = TRUE, quote = FALSE)
      hadoopWarnFile <- paste0(outputLoc, "/warnings/", warnFile)
      system(paste0("hdfs dfs -copyFromLocal -f ", warnFile, " ", hadoopWarnFile))
      quit("no", status = 0, runLast = TRUE)
    }
    # Only keep the variables I need for now and only those that have some good price data
    salesData <- subset(origSalesData, mdse_item_i %in% c(goodCounts$mdse_item_i), select = c("mdse_item_i", "wk_begin_date", "wk_end_date", "reg_retl_a", "own_retl_a",
          "avg_ext_sls_prc_unit", "retl_a", "med_customer_paid_price", "sls_retl_a", "sls_unit_q", "sales_storecount", "circular", "tpc", "pct_off_flag", "clearance_flag",
          "cpn_f",
          "pmtn_item_f",
          "tpr_f",
          "prc_guar_chit_f",
          "circ_f"))
    
    # We currently have a bug where the new promotion variable are null.  Set them to 0
    bugVars <- c("cpn_f", "pmtn_item_f", "tpr_f", "prc_guar_chit_f", "circ_f")
    salesData <- data.frame(salesData)
    salesData[c(bugVars)] <- sapply(salesData[c(bugVars)], function(x){ifelse(is.null(x), 0, x)})
    salesData <- data.table(salesData)
  }, error = function(error){
    errorMsg <- paste0("Error in reading sales file with error ", error, " ", key)
    show(errorMsg)
    show("No sales data for modeling")
    errorFile <- paste0("error_", key, ".txt")
    write.csv(errorMsg, errorFile, row.names = TRUE, quote = FALSE)
    hadoopErrorFile <- paste0(outputLoc, "/errors/", errorFile)
    system(paste0("hdfs dfs -copyFromLocal -f ", errorFile, " ", hadoopErrorFile))
    quit("no", status = 0, runLast = TRUE)
  })
  
  # We always want to forecast data with full weeks (eg. Sunday to Saturday).  For example, suppose
  # we have data up to Thursday, we will cut off the data to the previous Saturday.  This means the first
  # week of forecast contains actuals but we ignore the actuals.  
  endOfTraining <- NULL
  startOfTraining <- NULL
  # Use today's date and cut off the weeks prior to today
  # We assume that modeling and data collection happens on the same day
  currentDate <- Sys.Date()
  currentDay <- weekdays(currentDate)
  if(currentDay != "Saturday"){
      # Get day of week, with 1 = Sunday, 7 = Saturday
      dayNum <- wday(currentDate)
      lastSaturday <- currentDate - dayNum
      stopifnot(wday(lastSaturday) == 7)
      debugMsg <- paste0("New modeling end date is ", lastSaturday)
      startOfTraining <- lastSaturday - 130*7 + 1
      salesData <- subset(salesData, wk_end_date <= lastSaturday & wk_end_date >=startOfTraining)
      show(debugMsg)
      if(nrow(salesData) == 0){
        errorMsg <- paste0("No sales data available ", key)
        show(errorMsg)
        warnFile <- paste0("warn_", key, ".txt")
        write.csv(errorMsg, warnFile, row.names = TRUE, quote = FALSE)
        hadoopWarnFile <- paste0(outputLoc, "/warnings/", warnFile)
        system(paste0("hdfs dfs -copyFromLocal -f ", warnFile, " ", hadoopWarnFile))
        quit("no", status = 0, runLast = TRUE)
      } 
      if(nrow(salesData) < 10){
        errorMsg <- paste0("Insufficient data points.  Less than 10 data points", " ", key)
        show(errorMsg)
        simpleForecasts <- createSimpleForecasts(salesData, subGroups, lastSaturday + 7, lastSaturday, 
                                                 defaultWeeks = 8, numWeeksToForecast = 52)
        listOfObjects <- list(simpleForecasts = simpleForecasts, subGroups = subGroups)
        # Decide not to generate forecasts 
        #saveForScoring(outputLoc, listOfObjects, key)
        warnFile <- paste0("warn_sales_", key, ".txt")
        write.csv(errorMsg, warnFile, row.names = TRUE, quote = FALSE)
        hadoopWarnFile <- paste0(outputLoc, "/warnings/", warnFile)
        system(paste0("hdfs dfs -copyFromLocal -f ", warnFile, " ", hadoopWarnFile))
        debugMsg <- paste("Done with ", key)
        show(debugMsg)
        quit("no", status = 0, runLast = TRUE)
      }else{
        salesData <- subset(salesData, wk_end_date <= lastSaturday)
      }
  }else{
    lastSaturday <- currentDate
  }
  
  if(FALSE){
    #FAKE
    lastSaturday <- max(salesData$wk_end_date)
    startOfTraining <- lastSaturday - 130*7+1
  }
  salesData <- subset(salesData, wk_end_date <= lastSaturday)

  tryCatch({
    ptm <- proc.time()
    promoVars <- c("circular")
    modelData <- createWeeklyModelData(salesData, startOfTraining, lastSaturday, subGroups, promoVars, baseunitsWindow, outputLoc, key)
    ptm <- proc.time() - ptm
    show(paste0("Data creation time in seconds: ", ptm[3]))
  }, error = function(error){
    errorMsg <- paste0("Fatal error in creating weekly model data ", error)
    show(errorMsg)
    errorFile <- paste0("error_", key, ".txt")
    write.csv(errorMsg, errorFile, row.names = TRUE, quote = FALSE)
    hadoopErrorFile <- paste0(outputLoc, "/errors/", errorFile)
    system(paste0("hdfs dfs -copyFromLocal -f ", errorFile, " ", hadoopErrorFile))
    quit("no", status = 0, runLast = TRUE)
  })
  
  # Create the share model, both coeffs and training
  tryCatch({
    ptm <- proc.time()
    promoVars <- c("circular", "discount_33", "discount_40", "discount_50")
    useDefaultPriceDef <- TRUE
    shareModel_part1 <- createShareModel(modelData, promoVars, useDefaultPriceDef)
    shareModel <- getIntercepts(modelData, shareModel_part1, promoVars)
    ptm <- proc.time() - ptm
    show(paste0("Modeling time in minutes: ", ptm[3]/60))
  }, error = function(error){
    errorMsg <- paste0("Fatal error in estimating share model ", error)
    show(errorMsg)
    errorFile <- paste0("error_", key, ".txt")
    write.csv(errorMsg, errorFile, row.names = TRUE, quote = FALSE)
    hadoopErrorFile <- paste0(outputLoc, "/errors/", errorFile)
    system(paste0("hdfs dfs -copyFromLocal -f ", errorFile, " ", hadoopErrorFile))
    quit("no", status = 0, runLast = TRUE)
  })

  listOfObjects <- list(modelData = modelData, shareModel = shareModel, lastSaturday =lastSaturday, promoVars = promoVars, subGroups = subGroups)
  saveForScoring(outputLoc, listOfObjects, key)
  show(outputLoc)
  debugMsg <- paste0("Done with training ", key)
  show(debugMsg)
}


# Overload show function to write to the screen
show <- function(msg){
  write.table(msg, stderr())
}

trimws <- function (x, which = c("both", "left", "right")) 
{
    which <- match.arg(which)
    mysub <- function(re, x) sub(re, "", x, perl = TRUE)
    if (which == "left") 
        return(mysub("^[ \t\r\n]+", x))
    if (which == "right") 
        return(mysub("[ \t\r\n]+$", x))
    mysub("[ \t\r\n]+$", mysub("^[ \t\r\n]+", x))
}

while( length( x <- readLines(stdin,1) ) > 0 ) {
  value = trimws(strsplit(x, "\t")[[1]][2], c("both"))
  debugMsg <- paste0("This is value ", value)
  write.table(debugMsg, stderr())
  main(hive_db_dir, value)
}

