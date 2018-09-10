require(dplyr)
source("holdoutUtil.R")
# Create simple default forecasts for items that lack sufficient data.

# If a product coes not scan in the last defaultWeeks, then set the future forecast to some default number
# Everything else goes through additional processing because it may be a newly ramping up product, which is
# why it may not have sufficient data
# firstEndWeekOfHoldout refers to the first Saturday of the first week of holdout. We predict Sunday-Saturday
createSimpleForecasts <- function(inputSalesData, subGroups, firstEndWeekDateOfHoldout, lastDateOfTraining, 
                                 defaultWeeks = 8, numWeeksToForecast = 52){
  
  inputSalesData <- data.table(inputSalesData)
  inputSalesData <- inputSalesData[order(inputSalesData$mdse_item_i, inputSalesData$wk_begin_date),]
  inputSalesData <- inputSalesData[, recordNumber := seq(1, .N), by = c("mdse_item_i")]
  stats <- inputSalesData[, .(count = .N,
                              lastDate = max(wk_end_date),
                              meanStoreCount = mean(sales_storecount),
                              meanSales = mean(sls_unit_q)),
                          by = c("mdse_item_i")]
  
  inputSalesData2 <- merge(stats, inputSalesData, by = c("mdse_item_i"))
  inputSalesData2$weeksSinceTraining <- as.numeric((lastDateOfTraining - as.Date(inputSalesData2$lastDate)))/7
  inputSalesData2$storeGrowth <- inputSalesData2$sales_storecount/lag(inputSalesData2$sales_storecount) -1
  inputSalesData2$salesGrowth <- inputSalesData2$sls_unit_q/lag(inputSalesData2$sls_unit_q) -1
  
  # Divide the data for future forecasts.  If we have data in the last X weeks, where X = defaultWeeks, then we process it by
  # using the past data.  Otherwise, we consider the sku to be non-scanning and we just assign a default
  nonscanData <- subset(inputSalesData2, weeksSinceTraining >= defaultWeeks)
  nonscanForecasts <- generateNonScanItemsForecasts(nonscanData, firstEndWeekDateOfHoldout, numWeeksToForecast)
  scanData <- subset(inputSalesData2, weeksSinceTraining < defaultWeeks)
  scanForecasts <- generateScanItemsForecasts(scanData, firstEndWeekDateOfHoldout, lastDateOfTraining, numWeeksToForecast)
  combineForecasts <- rbind(scanForecasts, nonscanForecasts)

  return(combineForecasts)
}

checkDate <- function(predFile, forwardData){
  
  counts <- data.frame(table(predFile$mdse_item_i, predFile$forecast_date))  
  counts <- subset(counts, Freq > 1)
  if(nrow(counts) > 0){
      stop("Duplicates ", show(counts))
  }
  #stopifnot(nrow(counts) == nrow(forwardData))
}

# Generate forecasts for simple items
generateScanItemsForecasts <- function(input, firstEndWeekDateOfHoldout, lastDateOfTraining, numWeeksToForecast = 52){
  
  # If the last scan is over 50 and the growth of stores is greater than 10%, then assume that the product is growing and
  # take the ratio of units to store and ramp up the units until we hit about 1800 stores and then level off. 
  lastRecord <- subset(input, recordNumber == count)
  lastRecord$type <- with(lastRecord, ifelse(storeGrowth > 0.10 & sls_unit_q > 50, "growth", "sporadic"))
  sporadicItems <- subset(lastRecord, type == "sporadic" | is.na(type))
  growthItems <- subset(lastRecord, type == "growth")

  sporadicData <- subset(input, mdse_item_i %in% c(sporadicItems$mdse_item_i))
  sporadicForecasts <- generateSporadicItemsForecasts(sporadicData, firstEndWeekDateOfHoldout, lastDateOfTraining, numWeeksToForecast)
  growthData <- subset(input, mdse_item_i %in% c(growthItems$mdse_item_i))
  growthForecasts <- generateGrowthItemsForecasts(growthData, firstEndWeekDateOfHoldout, numWeeksToForecast)  
  combineForecasts <- rbind(sporadicForecasts, growthForecasts)
  stopifnot(nrow(combineForecasts) == length(unique(input$mdse_item_i))*numWeeksToForecast)
  return(combineForecasts)
}

generateGrowthItemsForecasts <- function(input, firstEndWeekDateOfHoldout, numWeeksToForecast){

  if (nrow(input) == 0) return (data.frame())
  lastRecord <- subset(input, recordNumber == count)
  lastRecord$numWeeksToFull <- pmax(round(log(1800/lastRecord$sales_storecount)/log(1 + lastRecord$storeGrowth)),1)
  # For each item, linearly project out growth based on numberOfWeeksToFull then keep it constant
  linearForecasts <- ddply(lastRecord, .(mdse_item_i), function(x){getLinearProjections(x, firstEndWeekDateOfHoldout, numWeeksToForecast)})
  # Check to make sure that the forecasts didn't blow up
  lastRecord <- subset(lastRecord, select = c("mdse_item_i", "sls_unit_q"))
  linearForecasts <- merge(linearForecasts, lastRecord, by = c("mdse_item_i"))
  linearForecasts$origPredictQty <- linearForecasts$predictQty
  linearForecasts$predictQty <- with(linearForecasts, ifelse(origPredictQty > 2.5*sls_unit_q, sls_unit_q*2.5, origPredictQty))
  finalForecasts <- subset(linearForecasts, select = c("mdse_item_i", "week", "predictQty", "wk_end_date"))
  return(finalForecasts)  
}

getLinearProjections <- function(input, firstEndWeekDateOfHoldout, numWeeksToForecast = 52){

  output <- data.frame()
  for (i in 1:input$numWeeksToFull){
    sales_storecount <- input$sales_storecount*(1+input$storeGrowth)**i
    output <- rbind(output, sales_storecount)
  }
  names(output) <- c("sales_storecount")
  output$predictQty <- output$sales_storecount/(input$sales_storecount/input$sls_unit_q)
  output$week <- seq(1, nrow(output))
  mostRecentQty <- as.numeric(subset(tail(output, 1), select = c("predictQty")))
  additionalWeeks <- data.frame()
  for (i in (nrow(output) + 1):numWeeksToForecast){
    additionalWeeks <- rbind(additionalWeeks, mostRecentQty*(1 + i/300))
  }
  names(additionalWeeks) <- c("predictQty")
  additionalWeeks$week <- seq(nrow(output) + 1, numWeeksToForecast)
  finalForecasts <- rbind.data.frame(output[, c("week", "predictQty")], additionalWeeks)
  # Add the week data
  finalForecasts$wk_end_date <- seq.Date(firstEndWeekDateOfHoldout, by = 7, length.out = numWeeksToForecast)
  return(finalForecasts)
}

generateSporadicItemsForecasts <- function(input, firstEndWeekDateOfHoldout, lastDateOfTraining, numWeeksToForecast = 52){
  
  if (nrow(input) == 0) return(data.frame())
  # Get data by quarter and predict future forecasts based on the four quarters averages
  # Get the last 52 weeks of data
  lastYear <- subset(input, wk_end_date >= as.Date(lastDateOfTraining) - 52*7)
  lastYear <- lastYear[order(lastYear$mdse_item_i, lastYear$wk_end_date),]
  lastYear$month <- month(lastYear$wk_end_date)
  lastYear$quarter <- with(lastYear, ifelse(month <=3, 1, ifelse(month <= 6, 2, ifelse(month <= 9, 3, 4))))
  lastYear <- data.table(lastYear)
  lastYearAvgSales <- lastYear[, .(avgSales = (sum(sls_unit_q, na.rm = TRUE)/13)), by = c("mdse_item_i", "quarter")]  
  quarter <- data.frame(quarter = c(1,2,3,4))
  grid <- merge(unique(lastYear$mdse_item_i), quarter)
  names(grid) <- c("mdse_item_i", "quarter")
  lastYearAvgSales <- merge(lastYearAvgSales, grid, by = c("mdse_item_i", "quarter"), all.y = TRUE)
  lastYearAvgSales[is.na(avgSales),]$avgSales <- 6/52
  # Now blow up the data for 52 weeks worth
  weekMapping <- data.frame(seq.Date(firstEndWeekDateOfHoldout, by = 7, length.out = numWeeksToForecast))
  weekMapping$week <- seq(1:nrow(weekMapping))
  names(weekMapping) <- c("wk_end_date", "week")
  blowUpForecasts <- merge(unique(lastYear$mdse_item_i), weekMapping)
  names(blowUpForecasts) <- c("mdse_item_i", "wk_end_date", "week")
  blowUpForecasts$month <- month(blowUpForecasts$wk_end_date)
  blowUpForecasts$quarter <- with(blowUpForecasts, ifelse(month <=3, 1, ifelse(month <= 6, 2, ifelse(month <= 9, 3, 4))))
  finalForecasts <- merge(blowUpForecasts, lastYearAvgSales, by = c("mdse_item_i", "quarter"), all.y = TRUE)
  finalForecasts <- subset(finalForecasts, select = c("mdse_item_i", "week", "wk_end_date", "avgSales"))
  names(finalForecasts)[4] <- c("predictQty")
  stopifnot(nrow(finalForecasts) == length(unique(input$mdse_item_i))*numWeeksToForecast)
  return(finalForecasts)  
}

generateNonScanItemsForecasts <- function(input, firstEndWeekDateOfHoldout, numWeeksToForecast = 52){

  if (nrow(input) == 0) return (data.frame())  
  
  items <- data.frame(table(input$mdse_item_i))
  items$mdse_item_i <- as.character(items$Var1)
  items <- subset(items, select = c("mdse_item_i"))
  week <- seq(1, numWeeksToForecast)
  # Cartesian join 
  items <- merge(items, week)
  names(items) <- c("mdse_item_i", "week")
  items <- items[order(items$mdse_item_i, items$week),]
  items$wk_end_date <- seq.Date(firstEndWeekDateOfHoldout, by = 7, length.out = numWeeksToForecast)
  # Assume it will sell X times a year, on average.  We are assuming that the item is dead product (not a seasonal product that will come back)
  items$predictQty <- 13/52
  # Check that the number of items in is the same as the number of rows coming out
  stopifnot(nrow(items) == length(unique(input$mdse_item_i))*numWeeksToForecast)
  return(items)    
}
