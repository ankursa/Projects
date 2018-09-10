require(data.table)
require(dplyr)
source("dataPrep.R")
source("determineRecalUse.R")

createWeeklyModelData <- function(salesData,startOfTraining, endOfTraining, subGroups, promoVars, baseunitsWindow = 8, outputLoc, key){

  # Determine beginning and ending dates, 
  if(FALSE){
    endOfTraining <- max(salesData$wk_end_date)
  }else{
    endOfTraining <- as.Date(ifelse(missing(endOfTraining) | is.null(endOfTraining), max(salesData$wk_end_date), endOfTraining))
  }
  
  # default to using last 130 weeks of data
  defaultStartDate <- as.Date(endOfTraining - 7*130 + 1)
  if(FALSE){
    startOfTraining <- defaultStartDate
  }else{
    startOfTraining <- as.Date(ifelse(missing(startOfTraining) | is.null(startOfTraining), defaultStartDate, startOfTraining))
  }

  debugMsg <- paste("Modeling dates (end of week): ", startOfTraining, " - ", endOfTraining)
  show(debugMsg)
  # To fit current infrastructure.
  salesData$co_loc_i <- -1
  # Create end of week date to confirm to current infrastructure
  salesData$sls_d <- salesData$wk_begin_date + 6
  salesData <- subset(salesData, sls_d >= startOfTraining & sls_d <= endOfTraining )
  
  # Get it to weekly level, with date being endOfWeek date (ie if a week is Sun-Sat, endOfWeek is Sat's value)
  mapping <- data.table(getWeeklyMapping(startOfTraining, endOfTraining))   
  datesDS <- aggregate(date ~ week, mapping, max)
  names(datesDS) <- c("week", "sls_d")
  # From the week data, next steps pads the data so that missing sales weeks are imputed with records
  show("Start fill out grid of days")
  inputDS <- data.table(salesData)
  fullGrid <- getFullGridOfData(inputDS, datesDS[c("sls_d")])
  fullGrid <- data.table(fullGrid)

  useRecalValues <- ddply(fullGrid, .(mdse_item_i), function(x){determineRecalUse(x, endOfTraining)})
  # Create baseunits at store-product level
  show("Start baseunits")
  baseunits <- createDailyBaseunits(fullGrid, subGroups, mapping, baseunitsWindow, promoVars = c(promoVars, "tpc"))
  weeklyBaseunits <- baseunits[, .(baseunits = sum(baseunits, na.rm = TRUE)), by = .(mdse_item_i, co_loc_i, week, subgroup)]
  weeklyBaseunits <- merge(weeklyBaseunits, datesDS, by = c("week"))
  weekly <- merge(fullGrid, weeklyBaseunits, by = c("mdse_item_i", "co_loc_i", "sls_d"))                               
  stopifnot(nrow(weekly) == nrow(fullGrid))
  
  # The seasonality will be created at the group-week level
  show("Start seasonality algorithm")
  seas <- createSeasonality(baseunits, datesDS)

  if(FALSE){
    plotSeasonality(seas)
    #plotBaseunitsAndQty(seas)
  }
  
  # Create data for sales model, which is at subgroup - store level.
  # For modeling, we typically don't want the entire 130 weeks (typically training weeks), just 65 weeks.  We needed 130 weeks 
  # for seasonality purposes.  Subset out the necessary number of data and renumber
  numModelingWeeks <- 65
  cutOffWeek <- max(seas$week) - numModelingWeeks +1
  seas2 <- data.table(subset(seas, week >= cutOffWeek))
  seas2$week <- seas2$week - numModelingWeeks
  # Add holidays
  holidayInfo <- addHolidays(weekly)
  weekly2 <- data.table(subset(holidayInfo$holidayDataSet, week >= cutOffWeek))
  weekly2$week <- weekly2$week - max(weekly2$week - numModelingWeeks)
  weekly2$subgroup <- as.character(weekly2$subgroup)
  # Merge seasonality and weekly data
  weekly2 <- merge(seas2[,c("subgroup", "week", "seas"), with = FALSE], weekly2, by = c("subgroup", "week"))
  dataStats <- throwOutInsufficientData(weekly2)  
  weekly3 <- dataStats$enough
  # Take out the insufficient skus from useRecalValues
  useRecalValues <- subset(useRecalValues, (mdse_item_i %in% c(dataStats$enough$mdse_item_i)))
  if(nrow(weekly3) == 0){
    errorMsg <- paste0("Not enough data for modeling ", key)
    show(errorMsg)
    simpleForecasts <- createSimpleForecasts(salesData, subGroups, endOfTraining + 7, endOfTraining, 
                                             defaultWeeks = 8, numWeeksToForecast = 52)
    listOfObjects <- list(simpleForecasts = simpleForecasts, subGroups = subGroups)
    # Decided not to create simple forecasts after all
    #saveForScoring(outputLoc, listOfObjects, key)
    warnFile <- paste0("warn_sales_", key, ".txt")
    write.csv(errorMsg, warnFile, row.names = TRUE, quote = FALSE)
    hadoopWarnFile <- paste0(outputLoc, "/warnings/", warnFile)
    system(paste0("hdfs dfs -copyFromLocal -f ", warnFile, " ", hadoopWarnFile))
    quit("no", status = 0, runLast = TRUE)
  } else{
    simpleForecasts <- data.frame()
  }
  # If some of the items don't have enough for modeling, get forecasts just for those items
  if(nrow(dataStats$notEnough) > 0){
      notEnoughItems <- subset(dataStats$notEnough, select = c("mdse_item_i"))
      notEnoughItems <- unique(notEnoughItems)
      insufficientData <- merge(salesData, notEnoughItems, by = c("mdse_item_i"))
      insufficientData <- insufficientData[order(insufficientData$mdse_item_i, insufficientData$wk_end_date),]
      #simpleForecasts <- createSimpleForecasts(insufficientData, subGroups, endOfTraining + 7, endOfTraining, 
      #                                         defaultWeeks = 8, numWeeksToForecast = 52)
      # Overwrite the current simple forecasts.  We want everything to go through modeling, if possible
      simpleForecasts <- data.frame()
  }
  # Rename sls_d to date to fit preexisting infrastructure
  index <- names(weekly3) == "sls_d"
  names(weekly3)[index] <- c("date")
  # Add daysOfZeroSales based on missing.sales.  This is to conform to existing infrastructure
  # Hard-code a number for now because the outlier detection algorithm uses this value so we want to give it a
  # constant number because we don't know the real number
  weekly3$daysOfZeroSales <- 0
  
  # If we don't have any promotion information, promoPrice will not exist.  Add it to the dataset
  promoPriceIndex <- which(names(weekly3) == "promoPrice")
  if(length(promoPriceIndex) == 0){
    weekly3$promoPrice <- weekly3$price
  }
  
  # Create different flags for discounts
  weekly3$discount <- with(weekly3, round( (baseprice - price)/baseprice, 2))
  # For now, only flag if discounts are >=25%, >=33%, >=40% and >=50%
  weekly3$discount_25 = with(weekly3, ifelse(discount >= 0.25 & discount < 0.33, 1, 0))
  weekly3$discount_33 = with(weekly3, ifelse(discount >= 0.33 & discount < 0.40, 1, 0))
  weekly3$discount_40 = with(weekly3, ifelse(discount >= 0.40 & discount < 0.50, 1, 0))
  weekly3$discount_50 = with(weekly3, ifelse(discount >= 0.50, 1, 0))
  
  return(list(seasData = seas2, weeklyStoreItemData = weekly3, holidaysList = holidayInfo$holidaysList, simpleForecasts = simpleForecasts, useRecalValues = useRecalValues, salesData = salesData))

}
