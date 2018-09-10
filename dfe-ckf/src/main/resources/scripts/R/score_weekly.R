rm(list=ls())
require(data.table)
require(methods)
require(plyr)
require(dplyr)
v<-View


stdin <- file("stdin", "r")

args <- commandArgs(trailingOnly = TRUE)
hive_db_dir <- args[1]
trainingLoc <- args[2]
outputLoc <- args[3]

source("holdoutUtil.R")
source("util.R")
source("dataPrepUtil.R")
source("createSimpleForecasts.R")

main <- function(hive_db_dir, key){

  # Read needed files first.  If it doesn't exist, exit right away.
  tryCatch({
    debugMsg <- paste0("Reading in listOfObjects from ", trainingLoc)
    show(debugMsg)
    listOfObjects <- readInTrainingOutput(trainingLoc, key)
  }, error = function(error){
    errorMsg <- paste0("Can't find listOfObjects for key =  ", key, " ", error)
    show(errorMsg)
    errorFile <- paste0("warn_sales_", key, ".txt")
    write.csv(errorMsg, errorFile, row.names = TRUE, quote = FALSE)
    hadoopErrorFile <- paste0(outputLoc, "/warnings/", errorFile)
    system(paste0("hdfs dfs -copyFromLocal -f ", errorFile, " ", hadoopErrorFile))
    quit("no", status = 0, runLast = TRUE)
  })
  
  namesList <- getInputNames(hive_db_dir, key)
  
  detailsFile <- file.path(paste0("promo_", key, ".txt")) 
  show(paste0("hadoop fs -getmerge ", namesList$promo, " ", detailsFile))
  system(paste0("hadoop fs -getmerge ", namesList$promo, " ", detailsFile))
  details <- tryCatch({
    origDetails <- fread(detailsFile, stringsAsFactors = FALSE, header = FALSE, na.strings = c("\\N", "NA", "NULL"), sep = "|")
    names(origDetails) <- c("promotionid", "mdse_item_i", "start_date", "end_date", "vehicle", "rewardtype", "rewardvalue", "storeCount")
    origDetails$start_date <- as.Date(origDetails$start_date)
    origDetails$end_date <- as.Date(origDetails$end_date)
    # Assume for now that storeCount == 1 corresponds to NULL, which is chain level. If so, set to maximum number of stores
    # I am seeing junk the promotion data.  We have promotions that end in 2050 (eg.promotionid = 639666124)
    # I have a few choices -- either throw them out or shorten the time frame.  I choose to throw them out
    origDetails$promoLength <- origDetails$end_date - origDetails$start_date + 1
    # Assume anything more than 7 weeks is not correct.
    origDetails$storeCount <- with(origDetails, ifelse(storeCount == 1, max(details$storeCount), storeCount))
    details <- subset(origDetails, promoLength <= 70)
  }, error = function(error){
    errorMsg <- paste0("Error in reading details file with error ", error)
    show(errorMsg)
    show("Will set the promotions to an empty file for now")
    warnFile <- paste0("warn_promo_", key, ".txt")
    write.csv(errorMsg, warnFile, row.names = TRUE, quote = FALSE)
    hadoopWarnFile <- paste0(outputLoc, "/warnings/", warnFile)
    system(paste0("hdfs dfs -copyFromLocal -f ", warnFile, " ", hadoopWarnFile))
    details <- data.table()
    return(details)
  })
  
  # Get the forward looking variables
  forwardData <- tryCatch({
    forwardFile <- file.path(paste0("forward_", key, ".txt")) 
    show(system(paste0("hadoop fs -getmerge ", namesList$forward, " ", forwardFile)))
    system(paste0("hadoop fs -getmerge ", namesList$forward, " ", forwardFile))
    origForwardData <- fread(forwardFile, stringsAsFactors = FALSE, header = FALSE, na.strings = c("\\N", "NA", "NULL"), sep = "|")
    names(origForwardData) <- c(
      "mdse_item_i",
      "holiday_array",
      "n_stores",
      "fcg_q",
      "retl_a",
      "max_promo_daynr",
      "max_promo_pctoff",
      "max_promo_dollaroff",
      "max_external_pctoff",
      "max_external_dollaroff",
      "circular",    # circular_flag
      "circular_flag_count",
      "tpc",   # tpc
      "tpc_flag_count",
      "clearance_flag",
      "clearance_flag_count",
      "dollar_off_flag",
      "dollar_off_flag_count",
      "qty_for_dollar_flag",
      "qty_for_dollar_flag_count",
      "free_product_flag",
      "free_product_flag_count",
      "pct_off_flag",
      "pct_off_flag_count",
      "giftcard_flag",
      "giftcard_flag_count",
      "external_promo_flag",
      "external_promo_flag_count",
      "pictured_flag",
      "pictured_flag_count",
      "min_pagenumber",
      "wk_begin_date",   # week_start_date
      "wk_end_date",     # week_end_date  
      "rel_d")
    origForwardData$wk_begin_date <- as.Date(origForwardData$wk_begin_date)
    origForwardData$wk_end_date <- as.Date(origForwardData$wk_end_date)
    forwardData <- subset(origForwardData, select = c("mdse_item_i", "n_stores", "wk_begin_date", "wk_end_date", "circular", "tpc"))
    weekMapping <- data.frame(seq.Date(from = (listOfObjects$lastSaturday + 1), to = max(forwardData$wk_begin_date), by = 7))
    names(weekMapping) <- c("wk_begin_date")
    weekMapping$week <- seq(1:nrow(weekMapping))
    forwardData <- merge(weekMapping, forwardData, by = c("wk_begin_date"))
    forwardData <- subset(forwardData, week <= 52)
  }, error = function(error){
    errorMsg <- paste0("Fatal error in reading forward file with error ", error)
    show(errorMsg)
    errorFile <- paste0("error_", key, ".txt")
    write.csv(errorMsg, errorFile, row.names = TRUE, quote = FALSE)
    hadoopErrorFile <- paste0(outputLoc, "/errors/", errorFile)
    system(paste0("hdfs dfs -copyFromLocal -f ", errorFile, " ", hadoopErrorFile))
    quit("no", status = 0, runLast = TRUE)
  })
    
  show(ls(listOfObjects))
  # It is possible that we didn't model at all and only have simple forecasts and subgroups.
  if(length(listOfObjects) == 2){
      show("Processing simple forecasts only")
      # Assume if there is one object, it must be the simple forecasts.  Just process that since there are no coefficients
      # so no need to go through the entire process of multiplying out the coefficients
      debugMsg <- paste0("List of objects ", ls(listOfObjects))
      show(debugMsg)

      prodSimpleForecasts <- prepSimpleForecasts(listOfObjects$simpleForecasts, forwardData, listOfObjects$subGroups)
      checkDate(prodSimpleForecasts, forwardData)
      writeForecastFile(key, outputLoc, "forecasts_", prodSimpleForecasts, includeColNames = FALSE)
      quit("no", status = 0, runLast = TRUE)
  }  
  
  modelData <- listOfObjects$modelData
  lastSaturday <- listOfObjects$lastSaturday
  promoVars <- listOfObjects$promoVars
  shareModel <- listOfObjects$shareModel
  salesData <- listOfObjects$modelData$salesData
  subGroups <- listOfObjects$subGroups
  
  trainingItems <- unique(shareModel$shareCoeffs$mdse_item_i)
  scoringItems <- unique(forwardData$mdse_item_i)
  sameSet <- intersect(trainingItems, scoringItems)
  if(length(sameSet) == 0){
    errorMsg <- paste0("No intersection between scoring and training sets ", key)
    show(errorMsg)
    errorFile <- paste0("error_", key, ".txt")
    write.csv(errorMsg, errorFile, row.names = TRUE, quote = FALSE)
    hadoopErrorFile <- paste0(outputLoc, "/errors/", errorFile)
    system(paste0("hdfs dfs -copyFromLocal -f ", errorFile, " ", hadoopErrorFile))
    quit("no", status = 0, runLast = TRUE)
  }
  
  
  # Generate the future results
  tryCatch({
    show("Starting to generate the future results")
    useRecalValues <- modelData$useRecalValues
    predictWithFuturePromo <- TRUE
    forecasts <- generateFutureForecasts(key, details, forwardData, shareModel, lastSaturday, promoVars, subGroups, numWeeksToForecast = 52, predictWithFuturePromo, useRecalValues)
    if(nrow(modelData$simpleForecasts) > 0){
      prodSimpleForecasts <- prepSimpleForecasts(modelData$simpleForecasts, forwardData, subGroups)
      forecasts$prodPreds <- rbind(prodSimpleForecasts, forecasts$prodPreds)
    }
    # Make sure every item gets a forecast.  It is possible that a item existed in this universe but doesn't have data in the training period
    # Set those forecasts to 0.10. Don't set it to 0 because the metrics heavily punish if you predict
    # 0 and you are wrong.
    missingSkus <- setdiff(unique(salesData$mdse_item_i), forecasts$prodPreds$mdse_item_i)
    missingSkus <- data.frame(missingSkus)
    # No longer do this for now
    if(FALSE){
    if(nrow(missingSkus) > 0){
    	show("Processing missing skus")
    	names(missingSkus)[1] <- c("mdse_item_i")
        missingData <- merge(salesData, missingSkus, by = c("mdse_item_i"))
        zeroForecasts <- createSimpleForecasts(missingData, subGroups, lastSaturday + 7, lastSaturday, 
                                               defaultWeeks = 8, numWeeksToForecast = 52)
        zeroForecasts$forecast_q <- 0.10
        prodZeroForecasts <- prepSimpleForecasts(zeroForecasts, forwardData, subGroups)
        forecasts$prodPreds <- rbind(forecasts$prodPreds, prodZeroForecasts)
    }
    }
    show("Sorting the file")
    forecasts$prodPreds <- forecasts$prodPreds[order(forecasts$prodPreds$mdse_item_i, forecasts$prodPreds$forecast_date),]
    # I don't know if I need this check anymore.  We use to check for the existence of 52 weeks
    checkDate(forecasts$prodPreds, forwardData)
    # I don't know if this is necessarily true anymore.  I suppose so because we should only be passing
    # items with data
    #stopifnot(length(unique(forecasts$prodPreds$mdse_item_i)) == length(unique(salesData$mdse_item_i)))
    writeForecastFile(key, outputLoc, "forecasts_", forecasts$prodPreds, includeColNames = FALSE)
    #writeForecastFile(key, outputLoc, "debug_forecasts_", forecasts$debugPreds, includeColNames = TRUE)
    debugMsg <- paste0("Done with ", key)
    show(debugMsg)
  }, error = function(error){
    errorMsg <- paste0("Fatal error in creating future forecasts ", error)
    show(errorMsg)
    errorFile <- paste0("error_", key, ".txt")
    write.csv(errorMsg, errorFile, row.names = TRUE, quote = FALSE)
    hadoopErrorFile <- paste0(outputLoc, "/errors/", errorFile)
    system(paste0("hdfs dfs -copyFromLocal -f ", errorFile, " ", hadoopErrorFile))
    quit("no", status = 0, runLast = TRUE)
  })
  
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

