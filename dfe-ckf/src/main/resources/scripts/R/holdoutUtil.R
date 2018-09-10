require(zoo)
require(dplyr)
source("shareModel.R")
source("flagHolidayWeeks.R")

# Reads in and populates needed output
readInTrainingOutput <- function(outputLoc, key){
  
  objectName <- paste0("listOfObjects_", key, ".rds")
  inputFile <- paste0(outputLoc, "/", objectName)
  system(paste0("hadoop fs -copyToLocal ", inputFile, " ", objectName))
  listOfObjects <- readRDS(objectName)

}

# Get the last five datapoints for each item and take the median of the baseprices
# This is the value that will be used by the future forecasts
# This is a utility function called by createForecastPrice
getPriceForForecasts <- function(input){
  
    lastDate <- max(input$date)
    allItems <- data.frame(unique(input$mdse_item_i))
    names(allItems) <- c("mdse_item_i")
    subsetData <- data.table(subset(input, date >= lastDate - 5*7))
    recentMedianBaseprices <- subsetData[, .(medianBaseprice = median(baseprice, na.rm = TRUE)), by = c("mdse_item_i")]    
    missingItems <- subset(merge(allItems, recentMedianBaseprices, all.x = TRUE, by = c("mdse_item_i")), is.na(medianBaseprice))
    if(nrow(missingItems) > 0){
      missingItemsData <- subset(input, mdse_item_i %in% c(missingItems$mdse_item_i), select = c("mdse_item_i", "week", "baseprice")) 
      missingItemsData <- missingItemsData[, maxWeek := max(week, na.rm = TRUE), by = c("mdse_item_i")]    
      missingItemsData <- subset(missingItemsData, week == maxWeek, select = c("mdse_item_i", "baseprice"))
      recentMedianBaseprices <- subset(recentMedianBaseprices, select = c("mdse_item_i", "medianBaseprice"))
      missingItemsData <- subset(missingItemsData, select = c("mdse_item_i", "baseprice"))
      names(missingItemsData) <- c("mdse_item_i", "medianBaseprice")
      finalPrices <- rbind.data.frame(recentMedianBaseprices, missingItemsData)
      stopifnot(nrow(finalPrices) == nrow(allItems))
      return(finalPrices)
    }else{
      return(recentMedianBaseprices)
    }
}

# Figure out which price to use for forecasting purpose. Logic is as follows:
# useBaseprices is on, we use the baseprice.  This takes precedence over all other values
# If useBaseprices = FALSE, we look to useDefaultPriceDef.  If this is on, we use the qty-weighted
# prices.  Otherwise, we use the non-weighted average prices.  The non-weighted prices takes an
# average of the store prices, which is calculated in the hive queries.
# Finally, weeks with promotion always use the promoted price, regardless of the settings.
# input refers to the holdout data.  It will append a variable called forecastPrice onto the input dataset
# trainingData refers to the modeling data. This data will be used to create the baseprice.
# This method is only valid if we assume we have some visibility into future prices
createForecastPrice <- function(input, trainingData, useDefaultPriceDef, useBaseprices){
  
  if(useBaseprices){
      forecastPrices <- getPriceForForecasts(trainingData)
      input <- merge(forecastPrices, input, by = c("mdse_item_i"), all.x = TRUE)
      promoPriceIndex <- which(names(input) == "promoPrice")
      if(length(promoPriceIndex) == 0){
        input$promoPrice <- input$medianBaseprice
      }
      
      input$forecastPrice <- with(input, ifelse(is.na(promoPrice), medianBaseprice, promoPrice))
      return(input)
  }
  
  if(useDefaultPriceDef){
    # This uses weighted price
    input$forecastPrice <- with(input, ifelse(is.na(promoPrice), price, promoPrice))
  }else{
    # This uses the average price, not weighted by total qty, but averaged across stores
    input$forecastPrice <- with(input, ifelse(is.na(promoPrice), avg_ext_sls_prc_unit, promoPrice))
  }

  return(input)
}

# Given a shareModel object, generate a forecast for production. Forecast
# will start the week after the training period, captured inside the shareModel
# and predict for the next 'numWeeksToForecast' weeks
generateFutureForecasts <- function(key, promotionData, forwardData, shareModel, endOfTraining,
                                    promoVars, subGroups, numWeeksToForecast = 52, predictWithFuturePromo = TRUE, useRecalValues){
  
    trainingData <- shareModel$itemData
    trainingData$mdse_item_i <- as.numeric(trainingData$mdse_item_i)
    trainingData <- subset(trainingData, !is.na(mdse_item_i))
    show("Generate future price")
    futurePrices <- getPriceForForecasts(trainingData)
    names(futurePrices) <- c("mdse_item_i", "baseprice")
    futurePrices$price <- futurePrices$baseprice
    show("Generate future seasonality")
    futureSeas <- generateFutureSeasonality(shareModel, subGroups, numWeeksToForecast)
    # Combine seasonality and prices
    futureData <- merge(futureSeas, futurePrices, by = c("mdse_item_i"))    
    futureData2 <- merge(futureData, forwardData, by = c("mdse_item_i", "week"))
    show("Generate future promotions")
    futureData2 <- generateFuturePromotions(promotionData, futureData2, endOfTraining, numWeeksToForecast)
    futureData2$discount <- with(futureData2, abs(price-baseprice)/baseprice)
    futureData2$discount_33 = with(futureData2, ifelse(discount >= 0.33 & discount < 0.40, 1, 0))
    futureData2$discount_40 = with(futureData2, ifelse(discount >= 0.40 & discount < 0.50, 1, 0))
    futureData2$discount_50 = with(futureData2, ifelse(discount >= 0.50, 1, 0))
      
    # If we don't think we should predict using future promotions because they are unstable, then set
    # prices to baseprice and zero out all promotion variables.  In terms of holdout testing, it may
    # be unreasonable to believe that we know what the promotions are when forecasting for a long forecast
    # horizon.
    if(!predictWithFuturePromo){
        show("Changing prices because we are not using future promotions")
        futureData2$price <- futureData2$baseprice
        futureData2[, c(promoVars)] <- 0
    }
    # Create trend and logPrice variable
    futureData2$trend <- 1
    futureData2$logPrice <- log(futureData2$price)
    # Get future holiday values
    temp <- addHolidays(futureData2, weekEndDateVar = "wk_end_date")
    futureData3 <- temp[[1]]
    holidaysList <- temp[[2]]
    if(length(holidaysList) == 0){
      # All holidays are missing.  Set it to 0
      futureData3[, c(shareModel$holidaysVar)] <- 0 
    } else if(length(holidaysList) < length(shareModel$holidaysVar)){
      missingHolidays <- setdiff(shareModel$holidaysVar, intersect(names(futureData3), shareModel$holidaysVar))
      if(length(missingHolidays) > 0){
        futureData3[, c(missingHolidays)] <- 0
      }
    }
    futurePreds <- getPredictedQty(futureData3, shareModel$shareCoeffs, promoVars, shareModel$holidaysVar)
    # Check large values
    futurePreds2 <- changeLargeForecasts(futurePreds, shareModel, promoVars, useRecalValues)
    # We want overforecasting rather than underforecasting, so check to see if recent weeks is much higher
    # than the predicted, if so, change the low predictions
    futurePreds3 <- changeSmallForecasts(futurePreds2, shareModel)
    # Change large promotion forecasts by looking at similar values same time last year
    futurePreds4 <- changePromoForecasts(futurePreds3, shareModel)
    # Adjust for fast moving forecasts based on recent weeks (either big increase and big decrease).  
    # Not working for BTS, table for now
    #futurePreds3 <- adjustFastMovingItems(futurePreds2, shareModel)
    prodPreds <- prepFuturePredsFileForProduction(futurePreds4, subGroups)
     
    # Keep for debugging  
    debugPreds <- futurePreds3
    debugPreds <- debugPreds[order(debugPreds$mdse_item_i, debugPreds$week),]
    debugPreds$maxStoreCount <- NULL
    debugPreds$subgroup.x <- NULL
    debugPreds$subgroup.y <- NULL
    return(list(debugPreds = debugPreds, prodPreds = prodPreds))
}

changePromoForecasts <- function(futurePreds3, shareModel){
  
  show("Change promo forecasts")
  # Find records with large circular or discounts greater than 20%
  promoWeeks <- subset(futurePreds3, circular.x > 0.50 | discount > 0.20, select = c("mdse_item_i", "week", "wk_begin_date", "circular.x", "discount", "price", "predictQty", "n_stores", "holiday"))
  show(names(shareModel$recalData$lastYearData))
  lastYearPromoWeeks <- subset(shareModel$recalData$lastYearData, circular > 0.5 | discount > 0.2, select = c("mdse_item_i", "wk_begin_date", "circular","discount", "price", "qty", "sales_storecount", "holiday"))
  names(lastYearPromoWeeks)[7] <- c("n_stores")
  names(lastYearPromoWeeks)[2] <- c("orig_date")
  likeRecords <- merge(promoWeeks, lastYearPromoWeeks, by = c("mdse_item_i"), allow.cartesian = TRUE)
  # Choose the records that are within one month of the forecast date
  likeRecords <- subset(likeRecords, abs(orig_date+365 - wk_begin_date) <= 28 & abs(n_stores.x/n_stores.y) < 1.15 & abs(n_stores.x/n_stores.y) > 0.85)
  promoForecasts <- ddply(likeRecords, c("mdse_item_i", "week"), chooseBestRecords)  
  if(nrow(promoForecasts) > 0){
    promoForecasts <- subset(promoForecasts, select = c("mdse_item_i", "week", "predictQty", "finalForecast"))
    names(promoForecasts) <- c("mdse_item_i", "week", "beforePromoAdjust", "promoForecast")
    adjustedForecasts <- merge(futurePreds3, promoForecasts, by = c("mdse_item_i", "week"), all.x = TRUE)
    adjustedForecasts$predictQty <- with(adjustedForecasts, ifelse(is.na(promoForecast), predictQty, promoForecast))
    return(adjustedForecasts)
  } else {
    return(futurePreds3)
  }
  
}

chooseBestRecords <- function(input){
  
    input$goodRecord <- with(input, ifelse(circular.x > 0 & circular > 0, 1, 0))
    input$goodRecord <- with(input, ifelse(price.x/price.y < 1.15 & price.x/price.y > 0.85, 1, goodRecord))
    input$goodRecord <- with(input, ifelse(abs(discount.x-discount.y) < 0.05, 1, goodRecord))
    input$new_date <- input$orig_date + 365
    input$weight <- as.integer(abs(input$new_date - input$wk_begin_date)/7) + 1
    input$weight <- 1/input$weight
    input <- data.table(input)
    input2 <- input[goodRecord == 1, .(predictQty = mean(predictQty), promoForecast = weighted.mean(qty)), by = c("mdse_item_i", "wk_begin_date")]
    input2$finalForecast <- with(input2, ifelse(predictQty/promoForecast < 0.85 | predictQty/promoForecast > 1.3, promoForecast, predictQty))
    return(input2)
}



changeSmallForecasts <- function(futurePreds2, shareModel){
  
  # We prefer overforecasting to underforecast. Check the recent data. If the upcoming forecasts look really low compared to the recent
  # weeks, then use the recent values to up the current one
  # Note that for seasonal items, this won't work well because recent weeks will be larger 
  # Weight the recent weeks more
  recentValues <- shareModel$recalData$recentWeeks
  recentValues <- subset(recentValues, is.na(outlier) | outlier == 0)
  recentValues$weights <- 1/(max(recentValues$week) -recentValues$week +1)
  recentValues$weights <- ifelse(recentValues$weights < 0, 1, recentValues$weights)
  recentValues <- recentValues[, .(medianValues = median(qty, na.rm = TRUE), meanValues = weighted.mean(qty, weights, na.rm = TRUE)), by = c("mdse_item_i")]
  recentValues$finalValues <- with(recentValues, medianValues*.80 + meanValues*0.2)
  recentValues$mdse_item_i <- as.integer(recentValues$mdse_item_i)
  futurePreds3 <- merge(futurePreds2, recentValues, by = c("mdse_item_i"), all.x = TRUE)
  # It is possible that sales_thisyear is not on the dataset.  If so, set it to avgRecentSales
  exists <- which((names(futurePreds3) == "sales_thisyear") == TRUE)
  if(length(exists) == 0){
     futurePreds3$sales_thisyear <-futurePreds3$avgRecentSales
  }
  futurePreds3$finalValues <- with(futurePreds3, ifelse(is.na(finalValues) & !is.na(sales_thisyear), sales_thisyear, finalValues))
  # Adjust the first 8 weeks only
  futurePreds3$beforeSmallAdj <- futurePreds3$predictQty
  futurePreds3$ratio <- with(futurePreds3, 0.5*beforeSmallAdj/sales_thisyear + 0.5*predictQty/finalValues)
  # Up the predictions if current prediction is lower than current year and final values.  This means we are probably underpredicting
  futurePreds3$predictQty <- with(futurePreds3, ifelse(week <=8 & beforeSmallAdj < sales_thisyear & beforeSmallAdj < finalValues & ratio < 0.82, 
          ( 1 + (1-ratio))*beforeSmallAdj, beforeSmallAdj))
  # Even after upping the predictions, we can still be way below.  if it is the case that we are still underforecasting by comparing
  # against recent values, up it again
  futurePreds3$predictQty <- with(futurePreds3, ifelse(week <=8 & predictQty/finalValues < 0.5, finalValues, predictQty))
  # Make sure we never get missing predictions.  In teh event that predictQty is missing,d efault back to beforeSmallAdj 
  futurePreds3$predictQty <- with(futurePreds3, ifelse(is.na(predictQty), beforeSmallAdj, predictQty))
  
  return(futurePreds3)
  
}


adjustFastMovingItems <- function(futurePreds2, shareModel){
  
  # Compare the upcoming forecasts to the recent weeks.  It shouldn't be too big or too small
  maxWeek <- max(shareModel$shareData$week)   
  inputSalesData <- subset(shareModel$shareData, week >= maxWeek - 5 & holiday == "no_holiday", select  = c("mdse_item_i", "outlier", "week", "date", "price", "baseprice", "qty"))
  inputSalesData$discount <- with(inputSalesData, (baseprice-price)/baseprice)  
  inputSalesData$flag1 <- 1
  
  # Get the upcoming forecasts
  upcomingPreds <- subset(futurePreds2, week <= 5, select = c("mdse_item_i", "week", "holiday", "price", "discount", "predictQty", "avgRecentSales"))
  upcomingPreds$flag1 <- 1
  combine <- merge(inputSalesData, upcomingPreds, by = c("mdse_item_i", "flag1"), all.y = TRUE)
  combine$flag1 <- NULL
  # Flag forecasts that are way out of line versus recent weeks
  combine <- combine[order(combine$mdse_item_i, combine$week.x, combine$week.y),]
  combine$perdiff <- with(combine, abs(qty-predictQty)/(qty+0.05))
  combine$diff <- with(combine, qty-predictQty)
  combine$flag <- with(combine, ifelse(abs(discount.x - discount.y) <= 0.05 & perdiff > 0.50, 1, 0))  
  #combine$flag <- with(combine, ifelse(outlier > 0, 0, 1))
  combine$flag <- with(combine, ifelse(holiday != "no_holiday", 0, flag))
  combine$flag <- with(combine, ifelse((qty <= 15 |is.na(qty)) & predictQty <= 15, 0, flag))
  combine$weight <- 1/( (maxWeek + combine$week.y) - combine$week.x + 1)
  combine$newWeight <- combine$perdiff*combine$weight
  
  needAdjust <- subset(combine, flag > 0)
  needAdjust <- data.table(needAdjust)
  newPrediction <- needAdjust[, .(adjustQty = weighted.mean(diff, w = newWeight, na.rm = TRUE), 
                                count = .N,
                                lastValues = toString(unique(qty))), 
                                by = c("mdse_item_i", "week.y")]

  flagCombine <- merge(newPrediction, combine[, c("mdse_item_i", "holiday", "week.y", "predictQty")], by = c("mdse_item_i", "week.y"), all.y = TRUE)
  flagCombine$mdse_item_i <- as.numeric(flagCombine$mdse_item_i)
  flagCombine[is.na(flagCombine$adjustQty),]$adjustQty <- 0
  flagCombine$adjustQty <- with(flagCombine, ifelse(count < 2, 0, adjustQty))
  flagCombine$origPredQtyBeforeFastAdj <- flagCombine$predictQty
  flagCombine$predictQty <- flagCombine$origPredQtyBeforeFastAdj + flagCombine$adjustQty
  flagCombine <- unique(flagCombine)  
  flagCombine$predictQty <- with(flagCombine, ifelse(predictQty < 1/13, 1/13, predictQty))
  # Adjust all future forecasts, based on the ratio
  ratios <- flagCombine[holiday=="no_holiday", .(ratio = mean(predictQty/origPredQtyBeforeFastAdj)), by = c("mdse_item_i")]

  # Piece together the forecasts, with the first 5 and the later weeks
  first5 <- subset(futurePreds2, week <= 5)
  first5$predictQty <- NULL
  flagCombine$week <- flagCombine$week.y
  first5 <- merge(flagCombine[,c("mdse_item_i", "week", "predictQty", "origPredQtyBeforeFastAdj"), with = FALSE], first5, by = c("mdse_item_i", "week"))
  first5$predictQty <- ifelse(is.na(first5$predictQty), first5$origPredQtyBeforeFastAdj, first5$predictQty)
  otherPredictions <- subset(futurePreds2, week > 5)
  otherPredictions <- merge(otherPredictions, ratios, by = c("mdse_item_i"), all.x = TRUE)  
  otherPredictions[is.na(otherPredictions$ratio),]$ratio <- 1
  otherPredictions$origPredQtyBeforeFastAdj <- otherPredictions$predictQty
  otherPredictions$predictQty <- otherPredictions$origPredQtyBeforeFastAdj*otherPredictions$ratio
  
  finalPredictions <- rbind(first5, otherPredictions, fill = TRUE)
  stopifnot(nrow(finalPredictions) == nrow(futurePreds2))
  return(finalPredictions)  
}

# It is possible that we still get very large values.  For each quarter, make sure that a forecast is never
# more than 3 times the average for the quarter
changeLargeForecasts <- function(input, shareModel, promoVars, useRecalValues){
    
  input <- data.table(input)
  input$quarter <- assignQuarter(input, dateVar = "wk_end_date")
  input <- input[, avgPred := mean(predictQty, na.rm = TRUE), by = c("mdse_item_i", "quarter")]
  input <- input[, sdPred := sd(predictQty, na.rm = TRUE), by = c("mdse_item_i", "quarter")]
  # In the event that sdPred is missing (which can happen because we can have 1 value) or if it is zero,
  # change the sdPred value to be 15% of the average predicted qty
  input$sdPred <- with(input, ifelse(is.na(sdPred) | sdPred < 0.05, avgPred*0.15, sdPred))
  input$origPredictQty <- input$predictQty
  input$predictQty <- with(input, ifelse(predictQty > avgPred + 4*sdPred, avgPred + 4*sdPred, origPredictQty))
  
  # It is possible that the intercept recalibration can go awry, particularly because the price that we recalibrated with
  # may be very different than the price we have now.  Make sure that the values are never more than the following rules:

  allRecalData <- shareModel$recalData$lastYearData
  allRecalData$quarter <- assignQuarter(allRecalData, dateVar = "date")
  allRecalData <- data.table(allRecalData)        
  allRecalData$mdse_item_i <- as.character(allRecalData$mdse_item_i)
  # Do this to get the coefficients on the dataset
  # We don't want to take the mean because of promo and holiday effects.  We want to get a baseline. 
  # Subtract out the effects of price and promotions and holidays and then take the mean.  
  # Do it off the actuals rather then using the predicted baseline because of errors in prediction.  We want to recalibrate to actuals
  # Rekey the data because share model intercepts are based on week
  allRecalData$origWeek <- allRecalData$week
  allRecalData$week <- allRecalData$recalWeek
  allRecalData <- getPredictedQty(allRecalData, shareModel$shareCoeffs, promoVars, shareModel$holidaysVar)
  allRecalData$predictBaseQty <- with(allRecalData, exp(recal_intercept + seas.x*seas.y + log(baseprice)*logPrice.y + trend.x*trend.y))
  allRecalData$newBaseQty <- with(allRecalData, predictBaseQty/predictQty*qty)                                    
  
  recentWeeks <- shareModel$recalData$recentWeeks
  # rekey the weeks.  Assume there are max of 65 weeks of training and seasonality is cyclical so week 53 is really week1, week 65 
  # is equivalent to week 13
  recentWeeks$week <- (recentWeeks$week-53)%%13 +1
  recentWeeks <- getPredictedQty(recentWeeks, shareModel$shareCoeffs, promoVars, shareModel$holidaysVar)
  recentWeeks$predictBaseQty <- with(recentWeeks, exp(recal_intercept + seas.x*seas.y + log(baseprice)*logPrice.y + trend.x*trend.y))
  recentWeeks$newBaseQty <- with(recentWeeks, predictBaseQty/predictQty*qty)                                    
  recentWeeks$quarter <- assignQuarter(recentWeeks)  
  
  recalQuarterAvgs <- allRecalData[, .(avgQuarterSales = mean(newBaseQty, na.rm = TRUE), sdQuarterSales = sd(newBaseQty, na.rm = TRUE)), by = c("mdse_item_i", "quarter")]
  recalRecentAvgs <- recentWeeks[, .(avgRecentSales = mean(newBaseQty, na.rm = TRUE), sdRecentSales = sd(newBaseQty, na.rm = TRUE)), by = c("mdse_item_i")]
  recalAvgs <- merge(recalQuarterAvgs, recalRecentAvgs, by = c("mdse_item_i"), all = TRUE)  
  recalAvgs$mdse_item_i <- as.integer(recalAvgs$mdse_item_i)
  input2 <- merge(input, recalAvgs, by = c("mdse_item_i", "quarter"), all.x = TRUE)
  # Choose the correct quantity based on the recal use values
  input2 <- merge(input2, useRecalValues, by = c("mdse_item_i"), all.x = TRUE)
  input2$avgSales <- with(input2, ifelse(is.na(useRecent) | useRecent == TRUE, avgRecentSales, avgQuarterSales))  
  input2$sdSales <- with(input2, ifelse(is.na(useRecent) | useRecent == TRUE, sdRecentSales, sdQuarterSales))  
  input2[is.na(input2$avgSales),]$avgSales <- 0
  input2[is.na(input2$sdSales),]$sdSales <- input2$avgSales*0.50
  input2[(input2$sdSales) < 0.05]$sdSales <- 5
  # These bounds should really be based on empirical evidence but I just used swags for now
  # Scenario 1: Make sure that the predictions stay within bounds for normal weeks
  input2$predictQty <- with(input2, ifelse(avgSales < 1000 & holiday == "no_holiday" & discount < 0.10 & predictQty > avgSales + 3*sdSales, avgSales + 3*sdSales, predictQty ))  
  input2$predictQty <- with(input2, ifelse(avgSales >= 1000 & avgSales < 2500 & holiday == "no_holiday" & discount < 0.10 & predictQty > avgSales + 2.5*sdSales, avgSales + 2.5*sdSales, predictQty ))  
  input2$predictQty <- with(input2, ifelse(avgSales >= 2500 & avgSales < 5000 & holiday == "no_holiday" & discount < 0.10 & predictQty > avgSales + 2.5*sdSales, avgSales + 2.5*sdSales, predictQty ))  
  input2$predictQty <- with(input2, ifelse(avgSales >= 5000 & holiday == "no_holiday" & discount < 0.10 & predictQty > avgSales + 2.5*sdSales, avgSales + 2.50*sdSales, predictQty))  
  # Scenario 2: If it is a holiday, then let it go higher
  input2$predictQty <- with(input2, ifelse(avgSales < 1000 & holiday != "no_holiday" & predictQty > avgSales + 6*sdSales, avgSales + 6*sdSales, predictQty ))  
  input2$predictQty <- with(input2, ifelse(avgSales >= 1000 & avgSales < 2500 & holiday != "no_holiday" & predictQty > avgSales + 6*sdSales, avgSales + 6*sdSales, predictQty ))  
  input2$predictQty <- with(input2, ifelse(avgSales >= 2500 & avgSales < 5000 & holiday != "no_holiday"  & predictQty > avgSales + 6*sdSales, avgSales + 6*sdSales, predictQty ))  
  input2$predictQty <- with(input2, ifelse(avgSales >= 5000 & holiday != "no_holiday" & predictQty > avgSales + 6*sdSales, avgSales + 6*sdSales, predictQty))  
  # Scenario 3: If it is on discount, then let it go higher
  input2$predictQty <- with(input2, ifelse(avgSales < 1000 & discount >= 0.10 & predictQty > avgSales + (6 + discount)*sdSales, avgSales + (6 + discount)*sdSales, predictQty ))  
  input2$predictQty <- with(input2, ifelse(avgSales >= 1000 & avgSales < 2500 & discount >= 0.10  & predictQty > avgSales + (6 + discount)*sdSales, avgSales + (6 + discount)*sdSales, predictQty ))  
  input2$predictQty <- with(input2, ifelse(avgSales >= 2500 & avgSales < 5000 & discount >= 0.10  & predictQty > avgSales + (6 + discount)*sdSales, avgSales + (6 + discount)*sdSales, predictQty ))  
  input2$predictQty <- with(input2, ifelse(avgSales >= 5000 & discount >= 0.10 & predictQty > avgSales + (6 + discount)*sdSales, avgSales + (6 + discount)*sdSales, predictQty))  

  return(input2)
  
}

prepSimpleForecasts <- function(simpleForecasts, forwardData, subGroups){
  
    # Need to get n_stores
    subsetForwardData <- subset(forwardData, select = c("mdse_item_i", "wk_end_date", "n_stores"))
    both <- merge(simpleForecasts, subsetForwardData, by = c("mdse_item_i", "wk_end_date"))
    both2 <- prepFuturePredsFileForProduction(both, subGroups)
    return(both2)
}

prepFuturePredsFileForProduction <- function(futurePreds4, subGroups){
  # Add columns that are required for the schema
  #mdse_item_i         	int                 	                    
  #ecom_item_i         	int                 	                    
  #tcin                	int                 	                    
  #dpci                	string              	DDD-CC-IIII         
  #forecast_date       	date                	YYYY-MM-DD          
  #location            	int                 	see column LOCATION_TYPE
  #store_count         	smallint            	number of stores in LOCATION
  #forecast_q          	float               	base+adjustment     
  #forecast_release_date	date                	YYYY-MM-DD          
  #forecast_granularity	smallint            	number of days      
  #model_id            	tinyint             	see table FORECAST_MODEL_NAMES
  #location_type       	tinyint             	see table FORECAST_LOCATION_TYPES
  
  # Partition Information	 	 
  # col_name            	data_type           	comment             
  
  #forecast_release_date	date                	YYYY-MM-DD          
  #forecast_granularity	smallint            	number of days      
  #model_id            	tinyint             	see table FORECAST_MODEL_NAMES
  #location_type       	tinyint             	see table FORECAST_LOCATION_TYPES

  prodPreds <- futurePreds4
  prodPreds$forecast_date <- prodPreds$wk_begin_date
  prodPreds$store_count <- prodPreds$n_stores
  prodPreds$forecast_q <- prodPreds$predictQty
  prodPreds <- subset(prodPreds, !is.na(n_stores))
  prodPreds$forecast_q <- ifelse(prodPreds$predictQty > 0, prodPreds$predictQty/prodPreds$store_count, 0/prodPreds$store_count)
  # We need week for ForecastTool but not production
  prodPreds <- subset(prodPreds, select = c("mdse_item_i", 
                               "forecast_date",
                               "store_count",
                               "forecast_q"))
  check <- subset(prodPreds, is.na(forecast_q) | is.nan(forecast_q))
  if(nrow(check) > 0){
    errorMsg <- head(check)
    show(errorMsg)
    if(nrow(check) != 0){
      stop("Missing values in forecast: ", errorMsg)
    }
  }
  
  return(prodPreds)
}


generateFuturePromotions <- function(promoDetails, futureData, endOfTraining, numWeeksToForecast = 52){
  
  if(nrow(promoDetails) == 0)
    return(futureData)
  
  # Make the reward type consistent
  promoDetails$rewardtype <- with(promoDetails, ifelse(rewardtype %in% c("PEROFF", "POFF", "PercentageOff", "PERO"), "POFF", rewardtype))
  promoDetails$vehicle <- with(promoDetails, ifelse(vehicle %in% c("CIRCULAR", "CRCLR"), "CIRCULAR", vehicle))
  # For now, only handle CIRCULAR and TPC
  promoDetails <- subset(promoDetails, vehicle %in% c("CIRCULAR", "TPC"))
  
  if(nrow(promoDetails) == 0){
    return(futureData)
  }
  
  #It is possible for multiple promotions to exist in one week with the same promoid but different rewardtype/rewardvalue
  # Take the one with the larger storeCount
  dedupe0 <- promoDetails[order(promoDetails$promotionid, as.numeric(promoDetails$mdse_item_i), -as.numeric(promoDetails$storeCount)),]
  dedupe0 <- cbind(dedupe0, first=0L)
  dedupe0 <- data.table(dedupe0)
  setkeyv(dedupe0, c("promotionid", "mdse_item_i"))
  dedupe0[dedupe0[unique(dedupe0),,mult="first", which=TRUE], first:=1L]
  dedupe0 <- subset(dedupe0, first == 1)
  dedupe0$first <- NULL
  #It is possible that multiple promotions exist for the same item, start-week but have different vehicles.  Take the one with a later promotionid
  #under the assumption that the later the promotionid means the later modified and intended promotion
  dedupe <- dedupe0[order(dedupe0$start_date, as.numeric(dedupe0$mdse_item_i), -as.numeric(dedupe0$promotionid)),]
  dedupe <- cbind(dedupe, first=0L)
  dedupe <- data.table(dedupe)
  setkeyv(dedupe, c("start_date", "mdse_item_i"))
  dedupe[dedupe[unique(dedupe),,mult="first", which=TRUE], first:=1L]
  dedupe <- subset(dedupe, first == 1)
  dedupe$first <- NULL
  # It is possible that the promotions span more than one week.  We define week as Sunday to Saturday.  If so, convert to one record per week
  # In other words, expand the promotions.  Note that you just can't take a look at the promotion length because it is possible to have
  # a promotion of 7 days and span two different Sun-Sat weeks.  We need to consider wk_begin_date and wk_end_date too
  dedupe$dayNum <- wday(dedupe$start_date)
  dedupe$wk_begin_date <- with(dedupe, ifelse(dayNum == 1, start_date, start_date - (dayNum - 1)))
  dedupe$wk_begin_date <- as.Date(dedupe$wk_begin_date)
  dedupe$wk_end_date <- as.Date(dedupe$wk_begin_date + 6)
  dedupe$dayNum <- NULL
  
  # Split the data into those that require extra processing, and those that are already within one week
  # flag == 1 means we need to process more
  dedupe$flag <- with(dedupe, ifelse(promoLength <= 7 & start_date >= wk_begin_date & end_date <= wk_end_date, 0, 1))
  needBlowOut <- subset(dedupe, flag == 1)
  dontNeedBlowOut <- subset(dedupe, flag == 0)
  debugMsg <- paste0(nrow(needBlowOut), " records need to be blown out")
  show(debugMsg)
  
  # Create additional records because promotions span more than one week
  blowOut <- function(input){
    debugMsg <- paste0("mdse_item_i = ", as.numeric(input[1, c("mdse_item_i")]), " promotionid = ", as.numeric(input[1, c("promotionid")]))
    #show(debugMsg)
    output <- input[1, ]
    current <- data.frame()
    control <- 1
    lastOutput <- tail(output, n = 1)
    repeat {
      lastOutput <- tail(output, n = 1)
      current <- input[1, ]
      current$wk_begin_date <- lastOutput$wk_begin_date + 7
      current$wk_end_date <- lastOutput$wk_end_date + 7
      output <- rbind.data.frame(output, current)
      if (current$wk_end_date >= current$end_date) break;
    }
    return(output)
  }
  
  if(nrow(needBlowOut) > 0){
    processedRecords <- ddply(needBlowOut, .(promotionid, mdse_item_i), blowOut)
    # After we blow out, we could have duplicates again.  Just take the later value again
    dedupe0 <- processedRecords
    dedupe <- dedupe0[order(dedupe0$wk_begin_date, as.numeric(dedupe0$mdse_item_i), -as.numeric(dedupe0$storeCount)),]
    dedupe <- cbind(dedupe, first=0L)
    dedupe <- data.table(dedupe)
    setkeyv(dedupe, c("wk_begin_date", "mdse_item_i"))
    dedupe[dedupe[unique(dedupe),,mult="first", which=TRUE], first:=1L]
    dedupe <- subset(dedupe, first == 1)
    dedupe$first <- NULL
    processedRecords <- rbind(dontNeedBlowOut, dedupe)
  }else{
    processedRecords <- dontNeedBlowOut
  }
  
  # In the event that it is still duplicated.  Just take one
  dedupe <- processedRecords
  dedupe2 <- dedupe[order(dedupe$wk_begin_date, as.numeric(dedupe$mdse_item_i)),]
  dedupe2 <- cbind(dedupe2, first=0L)
  dedupe2 <- data.table(dedupe2)
  setkeyv(dedupe2, c("wk_begin_date", "mdse_item_i"))
  dedupe2[dedupe2[unique(dedupe2),,mult="first", which=TRUE], first:=1L]
  dedupe2 <- subset(dedupe2, first == 1)
  dedupe2$first <- NULL
  processedRecords <- dedupe2
  
  # We can't simply replace the price because not all stores may have the promoted price and not all days in the week
  # have the promoted price.  Sometimes, it doesn't seem like the promotions are set up correctly because we have
  # a Sunday-Sunday promotions rather than ending on Saturday.  For now, exclude those promotions that are only 1 day of
  # the week.  Also, if a certain number percentage of stores have the promoted price, then just set everything to the promoted
  # price, even though this is not correct.  Technically, we should use a weighted average of prices, but since
  # that would require some idea of the future sales at store levels, that would be difficult.  Taking a straight
  # average of the prices isn't correct to, as it is likely that the promoted stores would have a higher proportion of sales.
  # Step 1: Figure out the number of days that the promotion is on during the week
  # For the purpose of the next step where we find the range, create a unique counter
  processedRecords$counter <- seq(1:nrow(processedRecords))
  # This step indentifies the days that start_date-end_date overlap with wk_begin_date-wk_end_date for each record
  range <- processedRecords[, .(intersect = as.Date(intersect(c(seq.Date(from = start_date, to = end_date, by = 1)),
                                                              c(seq.Date(from = wk_begin_date, to = wk_end_date, by = 1))))), by = c("counter")]
  
  # This counts up the records that are on promotion for that week
  numDays <- range[, .(numDays = .N), by = c("counter")]
  processedRecords2 <- merge(processedRecords, numDays, by = c("counter"))
  stopifnot(nrow(processedRecords2) == nrow(processedRecords))
  
  # Don't use if the number of promoted days is one and it starts on a Sunday.  Assume that we didn't set up the promotions properly
  processRecords2 <- subset(processedRecords2, numDays > 1 | (numDays == 1 & end_date != wk_begin_date))
  
  # Now convert these into prices.
  all <- merge(futureData,
               processedRecords2, by = c("mdse_item_i", "wk_begin_date", "wk_end_date"), all.x = TRUE)
  all$promoPrice <- with(all, ifelse(rewardtype %in% c("SALES", "REGRETAIL"), rewardvalue,
                                     ifelse(rewardtype == "POFF", signif( (1-(rewardvalue/100))*baseprice, 3),
                                            ifelse(rewardtype %in% c("DollarOff"), signif(baseprice-rewardvalue,3),
                                                   ifelse(rewardtype %in% c("Price Point", "QDLR", "PRPNT", "FixedPrice", "PRCPNT"), signif(rewardvalue, 3), NA))
                                     )))
  
  all$ratio <- all$storeCount/all$n_stores
  # If the number of promoted stores is less than half the number of available stores, then don't use the promoted price everywhere but
  # use a weighted average of an arbitrary set of weights instead
  all$promoPrice <- ifelse(all$promoPrice < 0.10, all$baseprice, all$promoPrice)
  all$price <- with(all, ifelse(!is.na(promoPrice) & ratio >= .50, promoPrice, ifelse(!is.na(promoPrice) & ratio < 0.5, (1-ratio)*promoPrice + ratio*baseprice, price)))
  all2 <- subset(all, select = c(names(futureData), "promoPrice"))
  return(all2)
  
}

# Generates the seasonality values for each item for the next numWeeksToForecast weeks
generateFutureSeasonality <- function(shareModel, subGroups, numWeeksToForecast = 52){
  
  # Get the seasonality onto the dataset
  seasData <- shareModel$itemData[, c("week", "seas", "mdse_item_i", "subgroup"), with = FALSE]
  seasData$mdse_item_i <- as.integer(seasData$mdse_item_i)
  # Exclude the data from the high level aggregations.  Only want subgroup information.  Last clause
  # is needed in the event that subgroup is a numeric
  seasData <- subset(seasData, (is.na(mdse_item_i) & !(subgroup %in% c("CATEGORY", "SUBGROUPS"))) | (subgroup == mdse_item_i))
  # Get the seasonality from one year ago.  This assumes the seasonality curve is cyclical.
  # assuming that we have 65 weeks of training, then we want week 66 and on, which 
  # corresponds to week 14
  maxWeek <- max(seasData$week)
  seasDataHoldout <- subset(seasData, week >=maxWeek - 52 + 1)
  # Possible that we exclude items from seasonality
  missingSubgroups <- setdiff(shareModel$shareCoeffs$subgroup, seasDataHoldout$subgroup)
  if(length(missingSubgroups) > 0){
    missingSeasSubgroup <- subset(seasData, subgroup %in% c(missingSubgroups))
    missingSeasSubgroup$week <- missingSeasSubgroup$week + 52
    seasDataHoldout <- rbind(missingSeasSubgroup, seasDataHoldout)
  }
  
  # Rekey the week to start at 1 to match the holdout data
  seasDataHoldout$week <- seasDataHoldout$week - min(seasDataHoldout$week) + 1
  seasDataHoldout$mdse_item_i <- NULL
  
  # It is possible that some of the seasonality at subgroup level may not have a full 52 weeks
  # If not, extend the values forward or backwards
  missingSeas <- subset(data.frame(table(seasDataHoldout$subgroup)), Freq < 52)
  missingSeas$Var1 <- as.character(missingSeas$Var1)
  missingSeasData <- subset(seasDataHoldout, subgroup %in% c(missingSeas$Var1))
  imputeSeas <- function(seas){
    weeks <- data.frame(week = seq(1, 52))
    seas <- merge(weeks, seas, by = c("week"), all.x = TRUE)
    seasForward <- na.locf(seas, na.rm = FALSE)
    seasBackwards <- na.locf(seas, na.rm = FALSE, fromLast = TRUE)
    concatSeas <- merge(seasForward, seasBackwards, by = c("week"), all = TRUE)
    concatSeas$week <- as.numeric(concatSeas$week)
    concatSeas$subgroup <- with(concatSeas, ifelse(is.na(subgroup.x), subgroup.y, subgroup.x))
    concatSeas$seas <- with(concatSeas, ifelse(is.na(seas.x), seas.y, seas.x))
    concatSeas <- subset(concatSeas, select = c("week", "subgroup", "seas"))
    return(concatSeas)
  }
  missingSeasDataImputed <- ddply(missingSeasData, .(subgroup), imputeSeas)
  seasDataHoldout2 <- rbind.data.frame(
                        subset(seasDataHoldout, !(subgroup %in% c(missingSeasDataImputed$subgroup))),
                        missingSeasDataImputed)        

  # If numWeeksToForecast > 52, recycle the seasonality 
  if(numWeeksToForecast > 52){
    additionalWeeks <- numWeeksToForecast - 52
    additionalSeas <- subset(seasDataHoldout2, week <= additionalWeeks)
    # Rekey so that the first week of the additional weeks start at 53 and count up from there
    additionalSeas$week <- additionalSeas$week + 52
    seasDataHoldout2 <- rbind.data.frame(additionalSeas, seasDataHoldout2)
  }
  seasDataHoldout2 <- seasDataHoldout2[order(seasDataHoldout2$subgroup, seasDataHoldout2$week),]
  
  # Get the coefficients and return the seasonality for each item that has coefficients
  items <- data.frame(unique(shareModel$shareCoeffs$mdse_item_i))
  names(items) <- c("mdse_item_i")
  items <- merge(items, subGroups[, c("mdse_item_i", "subgroup")], by = c("mdse_item_i"))

  finalSeasonality <- merge(seasDataHoldout2, items, by = c("subgroup"), allow.cartesian = TRUE)  
  finalSeasonality$seas <- as.numeric(finalSeasonality$seas)
  finalSeasonality$mdse_item_i <- as.numeric(as.character(finalSeasonality$mdse_item_i))
  finalSeasonality <- subset(finalSeasonality, week <= numWeeksToForecast)
  stopifnot(nrow(finalSeasonality) == nrow(items)*numWeeksToForecast)
  return(finalSeasonality)
}

# Write the forecast file to a hdfs location
writeForecastFile <- function(key, outputLoc, filePrefix, inputFile, includeColNames){
  localFile <- paste0(filePrefix, key, ".csv")
  hdfsFile <- paste0(outputLoc, "/fcsts/", localFile)
  write.table(inputFile, localFile, row.names = FALSE, quote = FALSE, col.names = includeColNames, sep = ",")
  cmd1 <- paste0("hdfs dfs -copyFromLocal -f ", localFile, " ", hdfsFile)
  write.table(cmd1, stderr())
  system(cmd1)
}

