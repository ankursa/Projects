getCoefficientsByQuarter <- function(recalCoeffs, holidaysVar, useRecalValues){
  
  q1holidays <- names(recalCoeffs$q1)[grep("hol_", names(recalCoeffs$q1))]
  q2holidays <- names(recalCoeffs$q2)[grep("hol_", names(recalCoeffs$q2))]
  q3holidays <- names(recalCoeffs$q3)[grep("hol_", names(recalCoeffs$q3))]
  q4holidays <- names(recalCoeffs$q4)[grep("hol_", names(recalCoeffs$q4))]
  
  finalQ1 <- subset(recalCoeffs$q1, select = c("mdse_item_i", "recal_intercept", q1holidays))  
  names(finalQ1)[c(1:2)] <- c("mdse_item_i", "recal_q1")
  finalQ2 <- subset(recalCoeffs$q2, select = c("mdse_item_i", "recal_intercept", q2holidays))
  names(finalQ2)[c(1:2)] <- c("mdse_item_i", "recal_q2")
  finalQ3 <- subset(recalCoeffs$q3, select = c("mdse_item_i", "recal_intercept", q3holidays))
  names(finalQ3)[c(1:2)] <- c("mdse_item_i", "recal_q3")
  # For Q4, choose the coefficients that yield higher results because we rather overforecast
  finalQ4 <- getQ4Coeffs(recalCoeffs, q4holidays)
  
  # Put together the intercepts and holidays and borrow the rest of the terms from any of
  # the datasets.  
  origVars <- names(recalCoeffs$recent)
  accountedForVars <- union(
    union(names(finalQ1), names(finalQ2)), 
    union(names(finalQ3), names(finalQ4))
  )
  # These are the ones we need to collect but we should throw out subgroup and any intercept terms first
  missingVars <- setdiff(origVars, accountedForVars)
  interceptIndices <- grep("tercept", missingVars)
  keepVars <- missingVars[-c(interceptIndices)]
  # If any of the keepVars are holidays, then just set the holidays to zero
  missingHolidayVars <- intersect(keepVars, holidaysVar)
  if(length(missingHolidayVars) > 0){
    keepVars <- setdiff(keepVars, missingHolidayVars)
  }
  # This gets the intercepts
  set1Coeffs <- merge(
    merge(finalQ1, finalQ2, by = c("mdse_item_i")),
    merge(finalQ3, finalQ4, by = c("mdse_item_i")), by = c("mdse_item_i"))
  if(length(missingHolidayVars) > 0){
    set1Coeffs[, c(missingHolidayVars)] <- 0
  }
  
  # This gets the rest of the missing coefficients
  # Theoretically, seas, trend, promo coefficients should be the same
  # across the 4 quarters
  set1Coeffs <- merge(set1Coeffs, recalCoeffs$q1[, c(keepVars, "mdse_item_i")])
  allHolidays <- union(union(q1holidays, q2holidays), union(q3holidays, q4holidays))
  allHolidays <- union(allHolidays, missingHolidayVars)
  newRecalCoeffs <- recalCoeffs$recent[, c("mdse_item_i", "recal_intercept", keepVars, allHolidays)]
  names(newRecalCoeffs)[c(1:2)] <- c("mdse_item_i", "recal_q1")
  newRecalCoeffs$recal_q2 <- newRecalCoeffs$recal_q1
  newRecalCoeffs$recal_q3 <- newRecalCoeffs$recal_q1
  newRecalCoeffs$recal_q4 <- newRecalCoeffs$recal_q1
  
  trueSkus <- subset(useRecalValues, V1 == TRUE, select = c("mdse_item_i"))
  falseSkus <- subset(useRecalValues, !(mdse_item_i %in% c(trueSkus$mdse_item_i)), select = c("mdse_item_i"))
  trueCoeffs <- subset(newRecalCoeffs, mdse_item_i %in% c(trueSkus$mdse_item_i))
  falseCoeffs <- subset(set1Coeffs, mdse_item_i %in% c(falseSkus$mdse_item_i))
  finalCoeffs <- rbind(trueCoeffs, falseCoeffs)  
  finalCoeffs <- finalCoeffs[order(finalCoeffs$mdse_item_i),]
  stopifnot(nrow(finalCoeffs) == nrow(recalCoeffs$recent))
  return(finalCoeffs)
}

getQ4Coeffs <- function(recalCoeffs, q4holidays){
  recentCoeffs <- data.frame(recalCoeffs$recent)
  # Higher coefficients due to holidays are not enough.  Just choose the one with higher intercept
  #recentCoeffs$recentHolidaySum <- apply(recentCoeffs[c("recal_intercept", q4holidays)], 1, sum)
  recentCoeffs$recentHolidaySum <- apply(recentCoeffs[c("recal_intercept")], 1, sum)
  q4Coeffs <- data.frame(recalCoeffs$q4)
  q4Coeffs$q4HolidaySum <- apply(q4Coeffs[c("recal_intercept", q4holidays)], 1, sum)
  combine <- merge(recentCoeffs[, c("mdse_item_i", "recentHolidaySum")], q4Coeffs[, c("mdse_item_i", "q4HolidaySum")], by = c("mdse_item_i"))
  combine$choose <- with(combine, ifelse(recentHolidaySum > q4HolidaySum, "recent", "q4"))
  recentCoeffsItems <- as.list(subset(combine, choose == "recent", select = c("mdse_item_i")))
  q4CoeffsItems <- as.list(subset(combine, choose == "q4", select = c("mdse_item_i")))
  
  set1Coeffs <- subset(recalCoeffs$recent, mdse_item_i %in% c(recentCoeffsItems[[1]]), select = c("mdse_item_i", "recal_intercept", q4holidays))
  set2Coeffs <- subset(recalCoeffs$q4, mdse_item_i %in% c(q4CoeffsItems[[1]]), select = c("mdse_item_i", "recal_intercept", q4holidays))
  
  finalSet <- rbind(set1Coeffs, set2Coeffs)
  names(finalSet)[c(1:2)] <- c("mdse_item_i", "recal_q4")
  stopifnot(nrow(finalSet) == nrow(q4Coeffs))
  return(finalSet)  
}

# Create different datasets for recalibration.  The thought process is that we want to have different time periods
# to calibrate our models.  Going into and immediately after the holiday season, we may want to use same time last year
# but all other times, we want to use recent weeks. We will divide the data based on quarters for holidays
createRecalDataSet <- function(priceVar, modelData, trainingData, holidaysVar){
  
  lastModelingDate <- max(modelData$weeklyStoreItemData$date)
  weeklyStoreItemData <- modelData$weeklyStoreItemData
  # Only fill in a small amount if the data around the missing is small.  Say if the average of the values is less than 8, then
  # fill in.  Otherwise, leave it blank
  weeklyStoreItemData <- ddply(weeklyStoreItemData, .(weeklyStoreItemData$mdse_item_i), fillInValues)
  maxWeek <- max(weeklyStoreItemData$week)
  recalDataSetForRecentWeeks <- createRecalDataSetRecentWeeks(priceVar, modelData, trainingData)

  # Line up the data
  lastYearData <- subset(weeklyStoreItemData, week > maxWeek -52 & holiday == "no_holiday")
  thisTimeLastYear <- lastModelingDate - 52*7
  minDate <- min(lastYearData$date)
  lastYearData$new_qty <- with(lastYearData, ifelse(is.na(qty) | qty == 0, 0.25, qty))
  lastYearData$recalWeek <- (lastYearData$date - thisTimeLastYear)/7
  lastYearData <- lastYearData[order(lastYearData$mdse_item_i, lastYearData$week),]
  lastYearData$logQty <- log(lastYearData$new_qty)
  lastYearData$price <- with(lastYearData, ifelse(is.na(price), baseprice, price))
  lastYearData[, priceVar] <- log(lastYearData$price)
  # When we recalibrate, use recent values
  lastYearData$origTrend <- lastYearData$week/max(lastYearData$week)
  lastYearData$trend <- 1
  lastYearData$new_qty <- NULL

  return(list(recentWeeks = recalDataSetForRecentWeeks, lastYearData = lastYearData))
}

fillInValues <- function(weeklyStoreItemData){
  
  input <- subset(weeklyStoreItemData, select = c("week", "mdse_item_i", "qty"))
  input <- data.table(input)
  input$indice <- input$week
  input[is.na(input$qty)]$indice <- NA
  # Get values before and after the missing
  input$qtyForward <- na.locf(input$qty, na.rm = FALSE)
  input$qtyBackward <- na.locf(input$qty, na.rm = FALSE, fromLast = TRUE)
  input$indiceForward <- na.locf(input$indice, na.rm = FALSE)
  input$indiceBackward <- na.locf(input$indice, na.rm = FALSE, fromLast = TRUE)
  input$qtyForward <- with(input, ifelse(abs(indiceForward - week) <= 6, qtyForward, NA))
  input$qtyBackward <- with(input, ifelse(abs(indiceBackward - week) <= 6, qtyBackward, NA))
  input$meanQty <- rowMeans(subset(input, select = c("qtyForward", "qtyBackward")), na.rm = TRUE)
  input$newQty <- with(input, ifelse(is.na(qty) & meanQty <= 5 | is.na(meanQty), 0.25, meanQty))
  weeklyStoreItemData$qty <- input$newQty
  return(weeklyStoreItemData)
}


fillInMissing <- function(lastYearData, holidaysVar){
  
  counts <- data.frame(table(lastYearData$mdse_item_i, lastYearData$quarter))
  counts <- subset(counts, Freq  == 0)
  if(nrow(counts) == 0){
    return(lastYearData)
  }
  counts$mdse_item_i <- as.integer(as.character(counts$Var1))
  counts$quarter <- as.integer(as.character(counts$Var2))
  counts <- counts[, c("mdse_item_i", "quarter")]
  augmentData <- merge(data.frame(lastYearData[, -c("quarter")]), counts, by = c("mdse_item_i"))
  # Zero out the holidays
  augmentData[, c(holidaysVar)] <- 0
  
  augmentData <- rbind(augmentData, lastYearData)
  
}

createRecalDataSetRecentWeeks <- function(priceVar, modelData, trainingData){
  
  lastModelingDate <- max(modelData$weeklyStoreItemData$date)
  lastModelingMonth <- month(lastModelingDate)
  weeklyStoreItemData <- modelData$weeklyStoreItemData
  # Need this line because we will assign a double to an integer value
  # Without it, R gives me snarky warning comments. I am not kidding -- seriously snarky.
  # Java is much kinder.
  weeklyStoreItemData$qty <- as.double(weeklyStoreItemData$qty)
  weeklyStoreItemData[is.na(weeklyStoreItemData$qty),]$qty <- 0.25
  maxWeek <- max(weeklyStoreItemData$week)
  
  recalData1 <- data.table(subset(weeklyStoreItemData, week >= maxWeek - 2 & holiday == "no_holiday"))
  recalData2 <- data.table(subset(trainingData, week >= maxWeek - 4 & holiday == "no_holiday"))
  counts1 <- recalData1[, .(origCount = .N), by = c("mdse_item_i")]
  counts1$mdse_item_i <- as.character(counts1$mdse_item_i)
  counts2 <- recalData2[, .(newCount = .N), by = c("mdse_item_i")]
  counts <- merge(counts1, counts2, by = c("mdse_item_i"), all.x = TRUE)
  counts[is.na(counts$newCount),]$newCount <- 0
  counts$choice <- with(counts, ifelse(newCount <=2, "recal1", "recal2"))
  recal1Skus <- as.list(subset(counts, choice == "recal1", select = c("mdse_item_i")))
  recal2Skus <- as.list(subset(counts, choice == "recal2", select = c("mdse_item_i")))
  
  recalData1 <- subset(recalData1, mdse_item_i %in% c(recal1Skus$mdse_item_i))
  recalData1$new_qty <- with(recalData1, ifelse(is.na(qty) | qty == 0, 0.25, qty))
  recalData1$logQty <- log(recalData1$new_qty)
  recalData1[, priceVar] <- log(recalData1$price)
  recalData1$trend <- recalData1$week/max(recalData1$week)
  recalData1$new_qty <- NULL
  
  recalData2 <- subset(recalData2, mdse_item_i %in% c(recal2Skus$mdse_item_i))  
  recalData <- rbind(recalData1, recalData2, fill = TRUE)
  recalData$price <- with(recalData, ifelse(is.na(price), baseprice, price))
  recalData[, priceVar] <- log(recalData$price)
  
  # Create different flags for discounts
  recalData$discount <- with(recalData, round( (baseprice - price)/baseprice, 2))
  # For now, only flag if discounts are >=25%, >=33%, >=40% and >=50%
  recalData$discount_25 = with(recalData, ifelse(discount >= 0.25 & discount < 0.33, 1, 0))
  recalData$discount_33 = with(recalData, ifelse(discount >= 0.33 & discount < 0.40, 1, 0))
  recalData$discount_40 = with(recalData, ifelse(discount >= 0.40 & discount < 0.50, 1, 0))
  recalData$discount_50 = with(recalData, ifelse(discount >= 0.50, 1, 0))
  
  return(recalData)  
}
