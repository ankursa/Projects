require(plyr)
require(dplyr)
source("flagHolidayWeeks.R")

adjustCoeffs <- function(coeffs, promoVars, priceVar, keyVar, useShareModel, holidaysVar){
  # In the event of missing values for coefficients, just set to 0
  coeffs[is.na(coeffs)] <- 0
  
  # Create item level coefficients based on a hierarchy
  candidates <- subset(coeffs, key != subgroup & !(subgroup %in% c("SUBGROUPS", "CATEGORY")))
  
  # For those with the correct signs for promo and price, just truncate and keep them.
  # Everything will go through recalibration
  # There are probably better ways out data based on the columns but I don't know how
  # Instead, I find the index belong to the column names that I care about
  priceCol <- which(names(coeffs) == priceVar)
  promoCols <- which(names(coeffs) %in% promoVars)
  # Select out coeffs with promos with the correct sign
  # promotions should have a positive sign.  
  coeffsPromo <- coeffs[Reduce(`&`, as.data.frame(coeffs[,c(promoCols)] >0)),]
  # price should have a negative sign
  goodCoeffs <- coeffsPromo[Reduce(`&`, as.data.frame(coeffsPromo[,c(priceCol)]  < 0)),]
  
  # For those that don't have the correct signs, "borrow" from the higher level of aggregation
  if(useShareModel){
    badCoeffs <- subset(candidates, key %in% c(setdiff(candidates$key, goodCoeffs$key)))
  } else{
    goodCoeffs$good <- 1    
    badCoeffs <- merge(candidates, goodCoeffs[c("subgroup", "key", "good")], by = c("subgroup", "key"), all.x = TRUE)
    badCoeffs <- subset(badCoeffs, is.na(good))    
    badCoeffs$good <- NULL
    goodCoeffs$good <- NULL
    stopifnot(nrow(badCoeffs) + nrow(goodCoeffs) == nrow(candidates))
  }
  # For bad_coeffs, use a hierarchy for the price and promo.  First use the subgroup, and then use category,
  # but only if category makes sense.  Otherwise, use predetermined numbers (average/median of category?)
  # or just predefined defaults
  # Add information from the category level
  goodCategory <- getCategoryCoeffs(goodCoeffs, priceVar, promoVars, holidaysVar, useShareModel)
  
  if(useShareModel){
    goodSubgroups <- subset(goodCoeffs, subgroup == key & key != "ALL")
  }else{
    goodSubgroups <- subset(goodCoeffs, key == -1 & subgroup != "CATEGORY")
  }
  
  if(nrow(goodSubgroups) == 0){
    goodSubgroups <- subset(goodCoeffs, subgroup == "SUBGROUPS" & key != "ALL")
    # If we have zero records, just set it to the category
    if(nrow(goodSubgroups) == 0) goodSubgroups <- goodCategory
  }
  names(goodSubgroups) <- paste0("subgroup_", names(goodSubgroups))
  names(goodSubgroups)[names(goodSubgroups) == "subgroup_subgroup"] <- c("subgroup")
  names(goodCategory) <- paste0("category_", names(goodCategory))
  
  newCoeffs <- merge(badCoeffs, goodSubgroups, by = c("subgroup"), all.x = TRUE)
  preRecalCoeffs <- newCoeffs
  subgroupPriceVar <- which(names(preRecalCoeffs) == paste0("subgroup_", priceVar))
  categoryPriceVar <- which(names(goodCategory) == paste0("category_", priceVar))
  
  priceVarBound <- -2.5
  preRecalCoeffs[,c(priceVar)] <- ifelse(preRecalCoeffs[,c(priceVar)] > 0, 
                                         ifelse(!is.na(preRecalCoeffs[, c(subgroupPriceVar)]) & preRecalCoeffs[, c(subgroupPriceVar)] < 0, 
                                                preRecalCoeffs[, c(subgroupPriceVar)],
                                                goodCategory[, c(categoryPriceVar)]), preRecalCoeffs[, c(priceVar)])
  preRecalCoeffs[,c(priceVar)] <- ifelse(preRecalCoeffs[, c(priceVar)] < priceVarBound, priceVarBound, preRecalCoeffs[, c(priceVar)])
  goodCoeffs[,c(priceVar)] <- ifelse(goodCoeffs[, c(priceVar)] < priceVarBound, priceVarBound, goodCoeffs[, c(priceVar)])
  
  # Do the same for the promoItemVars for each item in promoItemVars.
  # There is probably a way to do this in one shot, but I don't know how and I don't have the inclination to find out.
  # I am working on a weekend, for God's sakes.
  promoVarBound <- 2.0
  for(i in 1:length(promoVars)){
    subgroupPromoItemVar <- which(names(preRecalCoeffs) %in% paste0("subgroup_", promoVars[i]))
    categoryPromoItemVar <- which(names(goodCategory) %in% paste0("category_", promoVars[i]))
    
    preRecalCoeffs[,c(promoVars[i])] <- ifelse(preRecalCoeffs[,c(promoVars[i])] < 0, 
                                               ifelse(!is.na(preRecalCoeffs[, c(subgroupPromoItemVar)]) & preRecalCoeffs[, c(subgroupPromoItemVar)] > 0, 
                                                      preRecalCoeffs[, c(subgroupPromoItemVar)],
                                                      goodCategory[, c(categoryPromoItemVar)]), preRecalCoeffs[, c(promoVars[i])])
    preRecalCoeffs[,c(promoVars[i])] <- ifelse(preRecalCoeffs[, c(promoVars[i])] > promoVarBound, promoVarBound, preRecalCoeffs[, c(promoVars[i])])
    goodCoeffs[,c(promoVars[i])] <- ifelse(goodCoeffs[, c(promoVars[i])] > promoVarBound, promoVarBound, goodCoeffs[, c(promoVars[i])])
  }
  
  # Keep subgroup, but drop the rest of the columns from goodSubgroups
  preRecalCoeffs <- preRecalCoeffs[, !names(preRecalCoeffs) %in% setdiff(names(goodSubgroups), "subgroup")]
  # Add in the coefficients for the good items
  if(useShareModel){
    if(nrow(goodCoeffs) > 0)
      allPreRecalCoeffs <- rbind.fill(preRecalCoeffs, subset(goodCoeffs, subgroup != key & !subgroup %in% c("SUBGROUPS", "CATEGORY")))
    else
      allPreRecalCoeffs <- preRecalCoeffs
  }else{
    allPreRecalCoeffs <- rbind.fill(preRecalCoeffs, subset(goodCoeffs, key != -1))
  }
  # Truncate trend and seas, just in case they are really big.  Don't let this go too big or else we will have consistent
  # overforecasting or underforecasting, particularly as time passes.  As such, keep it very conservative
  allPreRecalCoeffs$seas <- with(allPreRecalCoeffs, ifelse(seas > 2.2, 2.2, ifelse(seas < -1.5, -1.5, seas))) 
  allPreRecalCoeffs$trend <- with(allPreRecalCoeffs, ifelse(trend > 2.2, 2.2, ifelse(trend < -1.5, -1.5, trend))) 
  
  return(allPreRecalCoeffs)
}

getRecalWeekData <- function(input, firstIndex, lastIndex){
  recalWeekData <- subset(input, recalWeek >= firstIndex & recalWeek <= lastIndex)
  while(nrow(recalWeekData) == 0){
    firstIndex <- firstIndex -1
    lastIndex <- lastIndex + 1
    recalWeekData <- subset(input, recalWeek >= firstIndex & recalWeek <= lastIndex)
  }
  return(recalWeekData)
}

getWeeklyIntercepts <- function(input, input2, priceVar, promoVars, keyVar, holidaysVar, useShareModel, useRecent = FALSE){
  
  #input <- subset(lastYearData, mdse_item_i == problemSku)
  #input2 <- allPreRecalCoeffs
  selectSku <- input[1, c("mdse_item_i")]
  selectCoeffs <- subset(input2, key == selectSku)
  debugMsg <- paste0("Processing ", selectSku)
  show(debugMsg)

  finalIntercepts <- NULL
  for(i in 1:52){
      # Need to experiment with these values
      firstIndex <- i-1
      lastIndex <- i+2
      recalWeekData <- getRecalWeekData(input, firstIndex, lastIndex)
      intercepts <- getRecalIntercepts(selectCoeffs, recalWeekData, priceVar, promoVars, keyVar, holidaysVar, useShareModel, useRecent = FALSE)
      index <- which(names(intercepts) == "recal_intercept")
      names(intercepts)[index] <- paste0("recal_intercept_", i)
      intercepts <- subset(intercepts, select = c("mdse_item_i", paste0("recal_intercept_", i)))   
      if (i == 1){
        finalIntercepts <- intercepts
      }else{
        finalIntercepts <- merge(finalIntercepts, intercepts, by = c("mdse_item_i"), all.x = TRUE)
      }
  }
  
  return(finalIntercepts)
}

prepareLastYearData <- function(recalData, useRecalValues){
  
  lastYearData <- recalData$lastYearData
  recentWeeks <- recalData$recentWeeks
  recentWeeks$mdse_item_i <- as.integer(recentWeeks$mdse_item_i)
  # Get the last record for recentWeeks
  recentWeeks <- recentWeeks[, maxWeek := max(week), by = c("mdse_item_i")]
  recentWeeks <- subset(recentWeeks, maxWeek == week, select = c("mdse_item_i", "baseprice"))
  names(recentWeeks) <- c("mdse_item_i", "recent_baseprice")
  stopifnot(!is.na(recentWeeks$recent_baseprice))
  stopifnot(nrow(recentWeeks) == nrow(useRecalValues))
  
  useOldPrices <- subset(useRecalValues, priceUseRecent == FALSE)
  lastYearPrices <- subset(lastYearData, mdse_item_i %in% c(useOldPrices$mdse_item_i))
  recentPrices <- subset(lastYearData, !(mdse_item_i %in% c(useOldPrices$mdse_item_i)))

  # Use the recent prices to last year's prices dataset and recalibrate with that, but only if the price
  # last year was not a promoted price
  recentPrices <- merge(recentPrices, recentWeeks, by = c("mdse_item_i"))
  recentPrices$logPrice <- with(recentPrices, ifelse(discount < 0.10, log(recent_baseprice), log(price)))
  if(nrow(lastYearPrices) > 0)
    lastYearPrices$recent_baseprice <- NA
  finalLastYearData <- rbind.data.frame(lastYearPrices, recentPrices)
  stopifnot(nrow(finalLastYearData) == nrow(lastYearData))
  
  return(finalLastYearData)
}

# Given the first pass of the coefficients, adjusts the bad-sign coefficients, borrows from other levels
# of aggregation
createCoeffs <- function(recalData, holidayData, coeffs, promoVars, priceVar, keyVar, holidaysVar, useShareModel, useRecalValues){
  
  allPreRecalCoeffs <- adjustCoeffs(coeffs, promoVars, priceVar, keyVar, useShareModel, holidaysVar)
  interceptsRecent <- getRecalIntercepts(allPreRecalCoeffs, recalData$recentWeeks, priceVar, promoVars, keyVar, holidaysVar, useShareModel, useRecent = FALSE)
  
  show("Prepare last year data")
  # Some items may have the same qty as last year but different prices.  If so, we should replace last year prices with this year prices
  # before recalibrating
  lastYearData <- prepareLastYearData(recalData, useRecalValues) 
  show("Getting weekly intercepts")
  weeklyIntercepts <- ddply(lastYearData, .(mdse_item_i), function(x){getWeeklyIntercepts(x, allPreRecalCoeffs, priceVar, promoVars, keyVar, holidaysVar, useShareModel, useRecent = FALSE)})
  interceptIndex <- which(names(allPreRecalCoeffs) == "(Intercept)")
  # Get the rest of the non-intercept coefficients back on the dataset
  weeklyIntercepts <- merge(weeklyIntercepts, allPreRecalCoeffs[, -c(interceptIndex)], by.x = c("mdse_item_i"), by.y = c("key"), all.x = TRUE)
 
  # Adjust the holiday variables because they may be way off after truncation. This may result in overfitting because we will
  # use the residuals to exactly hit the holiday values
  # The holidays need to be divided into when they occur to use the correct intercepts
  recalCoeffsRecent <- adjustHolidayCoeffs(interceptsRecent, priceVar, promoVars, holidaysVar, holidayData, keyVar)
  
  # The intercepts are based on holdout weeks, not regular weeks.  Remap so that the holidays are based on the future
  # The intercepts are based on holdout weeks, not regular weeks.  Remap so that the holidays are based on the future
  # We need to line up the holidays correctly in the future to make sure that the intercepts correspond correctly.  We didn't
  # need to do this above because the intercepts are all the same
  maxDate <- max(recalData$recentWeeks$date)
  futureHolidayWeeks <- data.frame(week = seq(1:52), wk_end_date = seq.Date(from = maxDate + 7, length.out = 52, by = 7))
  futureHolidayWeeks <- addHolidays(futureHolidayWeeks, weekEndDateVar = "wk_end_date")[[1]]
  futureHolidayWeeks <- subset(futureHolidayWeeks, holiday != "no_holiday", select = c("week", "wk_end_date", "holiday"))
  # Get the holidays and the weeks.  In the event of multiple weeks for the same holiday, choose earlier  week
  futureHolidayWeeks <- futureHolidayWeeks[order(futureHolidayWeeks$holiday, futureHolidayWeeks$week),]
  futureHolidayWeeks <- data.table(futureHolidayWeeks)
  futureHolidayWeeks <- futureHolidayWeeks[, count := seq(1:.N),
                               by = c("holiday")]  
  futureHolidayWeeks <- subset(futureHolidayWeeks, count == 1, select = c("holiday", "week", "wk_end_date"))
  futureHolidayWeeks <- data.frame(futureHolidayWeeks)
  holidayCoeffs <- NULL
  listOfWeeks <- unique(futureHolidayWeeks$week)
  for(i in 1:length(listOfWeeks)){
    coeffVar <- paste0("recal_intercept_", listOfWeeks[i])
    coeffIndex <- which(names(weeklyIntercepts) == coeffVar)
    if(length(coeffIndex) == 0){
      stop("Unexpected error: No variable named ", coeffVar, " found")
    }
    tempCoeffs <- weeklyIntercepts
    tempCoeffs$recal_intercept <- weeklyIntercepts[,c(coeffVar)]
    currentHoliday <- as.character(futureHolidayWeeks[i, c("holiday")])
    debugMsg <- paste("Processing ", currentHoliday)
    show(debugMsg)
    dropHolidaysVar <- setdiff(holidaysVar, paste0("hol_", currentHoliday))
    tempCoeffs <- subset(tempCoeffs, select = c(!(names(tempCoeffs) %in% c(dropHolidaysVar))))
    currentHolidayData <- subset(holidayData, holiday == currentHoliday)
    tempHoliday <- adjustHolidayCoeffs(tempCoeffs, priceVar, promoVars, paste0("hol_", currentHoliday), currentHolidayData, keyVar)
    tempHoliday$recal_intercept <- NULL
    if(i == 1){
        holidayCoeffs <- tempHoliday
    }else{
        holidayCoeffs <- merge(holidayCoeffs, tempHoliday[, c("mdse_item_i", paste0("hol_", currentHoliday))], by = c("mdse_item_i"), all.x = TRUE)      
    }
  }  
  
  # Check the dimensions.  We shouldn't have lost any items
  stopifnot(nrow(holidayCoeffs) == nrow(recalCoeffsRecent))
  stopifnot(nrow(holidayCoeffs) == nrow(allPreRecalCoeffs))

  return(list(recent = recalCoeffsRecent, weekly = holidayCoeffs))
}

# Gets the holidays for the given quarter
# It is possible we don't have data for the quarter
getHolidays <- function(holidayAssignment, quarterValue){
  holidayQuarterData <- subset(holidayAssignment, quarter == quarterValue)
  quarterlyHolidays <- unique(holidayQuarterData$holiday)
  if(length(quarterlyHolidays) > 0){
    quarterlyHolidays <- paste0("hol_", quarterlyHolidays)
    return(quarterlyHolidays) 
  }else{
    return(c())
  }
}

adjustHolidayCoeffs <- function(inputCoeffs, priceVar, promoVars, inputHolidaysVar, inputHolidayData, keyVar){
  
  if(length(inputHolidaysVar) == 0 ) return (inputCoeffs[,  -c(grep("hol_", colnames(inputCoeffs)))])
  
  preRecalCoeffs <- reorderDataByColName(inputCoeffs)
  inputHolidayData <- reorderDataByColName(inputHolidayData)
  modelingVars <- c(priceVar, promoVars, "seas", "trend")
  
  coeffsAndData <- merge(preRecalCoeffs[c(modelingVars, "mdse_item_i", "recal_intercept")], inputHolidayData[c(modelingVars, keyVar, "logQty", "week", "date", "holiday")], 
                         by.x = c("mdse_item_i"), by.y = c(keyVar), all.x = TRUE)  
  
  coeffIndices <- which(names(coeffsAndData) %in% paste(modelingVars, ".x", sep = ""))
  dataIndices <- which(names(coeffsAndData) %in% paste(modelingVars, ".y", sep = ""))
  # Get the multiplication of the coeff times the multiplier for all values in coeffsList
  values <-(coeffsAndData[c(coeffIndices)]*coeffsAndData[c(dataIndices)])
  coeffsAndData$sumValues <- apply(values, 1, sum) + coeffsAndData$recal_intercept
  coeffsAndData$residuals <- coeffsAndData$logQty - coeffsAndData$sumValues
  coeffsAndData$qty <- exp(coeffsAndData$logQty)
  coeffsAndData$holiday <- paste0("hol_", coeffsAndData$holiday)  
  
  # It is possible that we get multiple records.  Take the latest one
  coeffsAndData <- coeffsAndData[order(coeffsAndData$mdse_item_i, -coeffsAndData$week),]
  coeffsAndData <- data.table(coeffsAndData)
  coeffsAndData <- coeffsAndData[order(coeffsAndData$holiday, -coeffsAndData$week),]
  coeffsAndData <- coeffsAndData[, count := seq(1:.N),
                               by = c("mdse_item_i", "holiday")]  
  coeffsAndData <- subset(coeffsAndData, count == 1)
  coeffsAndData$count <- NULL

  holidayCoeffs <- inputCoeffs[,  -c(grep("hol_", colnames(inputCoeffs)))]

  for(i in 1:length(inputHolidaysVar)){
    currentCoeffs <- data.frame()
    var <- inputHolidaysVar[i]
    currentHol <- data.table(subset(coeffsAndData, holiday == var))
    currentCoeffs <- currentHol[, .(meanValue = mean(residuals, na.rm = TRUE)), by = c("mdse_item_i")]
    names(currentCoeffs)[2] <- var
  
    holidayCoeffs <- merge(holidayCoeffs, currentCoeffs, by = c("mdse_item_i"), all.x = TRUE)
  }
  # In the event of missing values for coefficients, just set to 0
  holidayCoeffs[is.na(holidayCoeffs)] <- 0
  # Make sure the holiday coefficient isn't too large in either direction
  # Make sure holidays are never negative
  for(i in 1:length(inputHolidaysVar)){
    var <- inputHolidaysVar[i]
    holidayCoeffs[, c(var)] <- ifelse(holidayCoeffs[,c(var)] > 5, 5, holidayCoeffs[, c(var)]) 
    holidayCoeffs[, c(var)] <- ifelse(holidayCoeffs[,c(var)] < 0, 0.1, holidayCoeffs[, c(var)]) 
  }
  return(holidayCoeffs)
}

# Recalibrate the intercepts
getRecalIntercepts <- function(preRecalCoeffs, recalDataInput, priceVar, promoVars, keyVar, holidaysVar, useShareModel, useItemModel = FALSE, useRecent = TRUE){
  
  #preRecalCoeffs <- selectCoeffs
  #recalDataInput <- recalWeekData
  #preRecalCoeffs <- allPreRecalCoeffs
  #recalDataInput <- recalData$recentWeeks
  recalDataInput <- data.table(recalDataInput)
  recalDataInput <- ddply(recalDataInput, .(mdse_item_i), function(x){identifyOutliers(x, varsOfInterest = c("qty"))})
  recalDataInput <- subset(recalDataInput, outlier == 0)  


  if(nrow(recalDataInput) == 0){
    postRecalCoeffs <- preRecalCoeffs
    postRecalCoeffs$mdse_item_i <- preRecalCoeffs$key
    postRecalCoeffs$recal_intercept <- preRecalCoeffs$"(Intercept)"    
    postRecalCoeffs$"(Intercept)" <- NULL
    postRecalCoeffs$key <- NULL
    return(postRecalCoeffs)  
  }
  
  modelingVars <- c(priceVar, promoVars, "seas", "trend", holidaysVar)
  preRecalCoeffs <- reorderDataByColName(preRecalCoeffs)
  recalDataInput <- reorderDataByColName(recalDataInput)
  if(useShareModel){
    coeffsAndData <- merge(preRecalCoeffs[c(modelingVars, "key")], recalDataInput[c(modelingVars, keyVar, "logQty", "week", "date")], 
                           by.x = c("key"), by.y = c(keyVar), all.x = TRUE)  
    coeffsAndData$newKey <- coeffsAndData$key
    preRecalCoeffs$newKey <- preRecalCoeffs$key
  }else if(useItemModel){
    coeffsAndData <- merge(preRecalCoeffs[c(modelingVars, "mdse_item_i", "key")], recalDataInput[c(modelingVars, "mdse_item_i", keyVar, "logQty", "week", "date")], 
                           by.x = c("mdse_item_i", "key"), by.y = c("mdse_item_i", keyVar), all.x = TRUE)  
    coeffsAndData$newKey <- with(coeffsAndData, paste0(mdse_item_i, "||", key))
    preRecalCoeffs$newKey <- with(preRecalCoeffs, paste0(mdse_item_i, "||", key))
  }else{
    coeffsAndData <- merge(preRecalCoeffs[c(modelingVars, "subgroup", "key")], recalDataInput[c(modelingVars, "subgroup", keyVar, "logQty", "week", "date")], 
                           by.x = c("subgroup", "key"), by.y = c("subgroup", keyVar), all.x = TRUE)  
    coeffsAndData$newKey <- with(coeffsAndData, paste0(subgroup, "||", key))
    preRecalCoeffs$newKey <- with(preRecalCoeffs, paste0(subgroup, "||", key))
  }
  coeffIndices <- which(names(coeffsAndData) %in% paste(modelingVars, ".x", sep = ""))
  dataIndices <- which(names(coeffsAndData) %in% paste(modelingVars, ".y", sep = ""))
  # Get the multiplication of the coeff times the multiplier for all values in coeffsList
  values <- coeffsAndData[c(coeffIndices)]*coeffsAndData[c(dataIndices)]
  coeffsAndData$sumValues <- apply(values, 1, sum)
  coeffsAndData$residuals <- coeffsAndData$logQty - coeffsAndData$sumValues
  coeffsAndData$qty <- exp(coeffsAndData$logQty)
  
  # In the event that we only have one key value, the regression will fail.  Make it pass by creating a duplicate of the current
  # data and creating a fake key.  
  numUniqueKeys <- length(unique(coeffsAndData$key))
  if(numUniqueKeys == 1 | nrow(coeffsAndData < 2)){
    duplicateData <- coeffsAndData
    duplicateData$key <- "FakeKey"
    duplicateData$newKey <- "FakeKey"
    coeffsAndData <- rbind.data.frame(coeffsAndData, duplicateData, duplicateData)
  }
  coeffsAndData <- data.table(coeffsAndData)
  #formula <- paste("residuals", " ~ 1 + (1|newKey)")
  #intModel1 <- lmer(formula, data = coeffsAndData )
  #intercepts1 <- coef(intModel1)$newKey[1]
  #names(intercepts1) <- c("recal_intercept1")
  #intercepts1$newKey <- rownames(intercepts1)
  intercepts1 <- ddply(coeffsAndData, .(newKey), function(x){mean(x$residuals, na.rm = TRUE)})
  intercepts1$newKey <- rownames(intercepts1)
  names(intercepts1) <- c("newKey", "recal_intercept1")

  # This uses the mean of the residuals but for a shorter time frame.  Don't do the following.  Just use the entire time frame
  # and hope seasonality will adjust it accordingly
  if(useRecent){
    coeffsAndData <- data.table(subset(coeffsAndData, week >= max(coeffsAndData$week) -6))
  }
  
  intercepts2 <- ddply(coeffsAndData, .(newKey), function(x){wtd.mean(x$residuals, weights = x$qty, na.rm = TRUE)})
  names(intercepts2) <- c("newKey", "recal_intercept2")
  #intercepts2 <- coeffsAndData[, .(recal_intercept2 = mean(residuals, na.rm = TRUE)), by = c("newKey")]
  postRecalCoeffs <- merge(preRecalCoeffs, intercepts1, by = c("newKey"), all.x = TRUE)
  postRecalCoeffs <- merge(postRecalCoeffs, intercepts2, by = c("newKey"), all.x = TRUE)
  postRecalCoeffs$recal_intercept <- with(postRecalCoeffs, ifelse(is.na(recal_intercept2), recal_intercept1, recal_intercept2))
  postRecalCoeffs$key <- NULL
  postRecalCoeffs$mdse_item_i <- postRecalCoeffs$newKey
  postRecalCoeffs$newKey <- NULL
  return(postRecalCoeffs)
  
}

# Borrowed from Hmisc:wtd.mean because we can't load libraries
wtd.mean <- function (x, weights = NULL, normwt = "ignored", na.rm = TRUE) 
{
  if (!length(weights)) 
    return(mean(x, na.rm = na.rm))
  if (na.rm) {
    s <- !is.na(x + weights)
    x <- x[s]
    weights <- weights[s]
  }
  sum(weights * x)/sum(weights)
}
