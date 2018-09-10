require(plyr)
require(dplyr)
require(data.table)
require(ggplot2)
source("modelingUtil.R")
source("itemLevel.R")
source("interceptRecalibration.R")
source("createRecal.R")

# useDefaultPrice = TRUE uses the default definition, which is the weighted price
createShareModel <- function(modelData, promoVars, useDefaultPriceDef = TRUE){
  
  # Note that we throw out the zero-valued records inside createItemData, which means there is a bias in the model
  # and that we may not have all weeks after this step
  holidaysVar <- paste0("hol_", modelData$holidaysList)
  
  nonDiscountPromoVars <- setdiff(promoVars, c("discount_33", "discount_40", "discount_50"))
  itemData <- createItemData(modelData$weeklyStoreItemData, nonDiscountPromoVars, holidaysVar)
  # Create different flags for discounts
  itemData$discount <- with(itemData, round( (baseprice - price)/baseprice, 2))
  # For now, only flag if discounts are >=25%, >=33%, >=40% and >=50%
  itemData$discount_25 = with(itemData, ifelse(discount >= 0.25 & discount < 0.33, 1, 0))
  itemData$discount_33 = with(itemData, ifelse(discount >= 0.33 & discount < 0.40, 1, 0))
  itemData$discount_40 = with(itemData, ifelse(discount >= 0.40 & discount < 0.50, 1, 0))
  itemData$discount_50 = with(itemData, ifelse(discount >= 0.50, 1, 0))
  
  # Create a trend variable
  itemData$trend <- itemData$week/max(itemData$week)
  # First model, just take log-log.  Don't do anything with discount for now
  # Note that logQty is the sum of the qty for the individual mdse_item_i or all the mdse_item_i at the subgroup levels, 
  # which might not be correct when you have assortment changes or varying number of stores carrying the product
  # For example, for CHEESE
  
  priceVar <- "logPrice"
  # We threw out any qty = 0 records so we can take log(qty) safely
  itemData$logQty <- with(itemData, log(qty))
  if(useDefaultPriceDef){
    itemData[, c(priceVar)] <- with(itemData, log(price))
  }else{
    itemData[, c(priceVar)] <- with(itemData, log(avg_ext_sls_prc_unit))
  }  
  
  #trainingData <- subset(itemData, outlier == 0)
  # When the majority of the outliers are at the end of the training period, we don't want to throw those records out.  Instead, we will keep that
  # data and throw out the 'non-outlier' records that preceded the outliers.  In other words, we assume that the data at the end model
  # indicates that we are in a different regime so keep those.cleanSeries algorithm decides which records to keep
  numTrainingWeeks <- max(itemData$week)
  origTrainingData <- ddply(itemData, c("mdse_item_i"), function(x){cleanSeries(x, numTrainingWeeks, var = "qty", span = 8, runThreshold = 5, windowSize = 5, holidaysVar)})
  # It is possible that we throw out a record because it is on discount or promo.  If so, keep it.  The promo level has to be above 50% and discount
  # must be at least 10% to be kept
  origTrainingData <- subset(origTrainingData, select = c("mdse_item_i", "week", "qty"))
  names(origTrainingData)[3] <- c("origQty")
  trainingData <- merge(origTrainingData, itemData, by = c("mdse_item_i", "week"), all.y = TRUE)
  # Note that we have hard-coding here with circular
  trainingData$keep <- with(trainingData, (!is.na(origQty) | (holiday != "no_holiday" & !is.na(qty)) | (circular > 0.50 & !is.na(qty)) | ((baseprice-price)/baseprice > .10 & !is.na(qty))), 1, 0)
  trainingData <- subset(trainingData, keep == TRUE)
  trainingData$keep <- NULL
  trainingData$origQty <- NULL
  
  # Run a mixed model regression at subgroup level, with random effect at item level
  coeffs <- ddply(trainingData, .(trainingData$subgroup), function(x){runModelRegression(x, promoVars, randomKey = "mdse_item_i", priceVar = "logPrice", depVar = "logQty", holidaysVar = holidaysVar)})
  names(coeffs)[1] <- c("subgroup")
  # In the event that trend wasn't created for any subgroup, put trend on the dataset
  trendIndex <- which(names(coeffs) == c("trend"))
  if(length(trendIndex) == 0){
    coeffs$trend <- 0
  }
  if(nrow(subset(coeffs, is.na(trend))) > 0) coeffs[is.na(coeffs$trend),]$trend <- 0
  
  # In the event that seas wasn't created for any subgroup, put trend on the dataset
  seasIndex <- which(names(coeffs) == c("seas"))
  if(length(seasIndex) == 0){
    coeffs$seas <- 0
  }
  if(nrow(subset(coeffs, is.na(seas))) > 0) coeffs[is.na(coeffs$seas),]$seas <- 0
  
  if(sum(promoVars %in% names(coeffs)) < length(promoVars) & sum(promoVars %in% names(coeffs)) != 0){
    # This only works if promovars was estimated for some items but not others
    # Find the missing coefficients and set them to 0
    coeffsNames <- names(coeffs)
    missingPromoVars <- setdiff(promoVars, intersect(coeffsNames, promoVars))
    # If we are missing a column, set it to a standard value
    if(length(missingPromoVars) > 0){
        coeffs[, c(missingPromoVars)] <- NA
    }
    coeffs[c(promoVars)] <- sapply(coeffs[c(promoVars)], function(x){ifelse(is.na(x), 1.1, x)})
  }else{
    # We reach here if promotion was missing everywhere.  If that is the case, just set the promotion values to some default
    coeffs[, c(promoVars)] <- 1.1
  }

  if(sum(holidaysVar %in% names(coeffs)) == length(holidaysVar)){
    # This only works if holidaysList was estimated for some items but not others
    coeffs[c(holidaysVar)] <- sapply(coeffs[c(holidaysVar)], function(x){ifelse(is.na(x), 0, x)})
  }else{
    # We reach here if holidays was constant everywhere.  If that is the case, just set the holidays values to zero
    coeffs[, c(holidaysVar)] <- 0
  }

  # It is possible we don't have price coefficient on the model (ie constant values).  If not, just set to -0.8 as a default.  We do this
  # to assume that these products are not elastic since we never change prices
  priceCol <- which(names(coeffs) == priceVar)
  if(length(priceCol) == 0){
    coeffs[, c(priceVar)] <- -0.8
    priceCol <- which(names(coeffs) == priceVar)
  }
  
  return(list(trainingData = trainingData, itemData = itemData, coeffs = coeffs, holidaysVar = holidaysVar))
  
}

getIntercepts <- function(modelData, firstSet, promoVars, priceVar = "logPrice"){
  
  # Determine recal dataset
  trainingData <- firstSet$trainingData
  coeffs <- firstSet$coeffs
  holidaysVar <- firstSet$holidaysVar
  itemData <- firstSet$itemData
  
  recalData <- createRecalDataSet(priceVar, modelData, trainingData, firstSet$holidaysVar)
  holidayData <- subset(modelData$weeklyStoreItemData, holiday != "no_holiday")
  holidayData$logQty <- with(holidayData, ifelse(is.na(qty)|qty == 0, log(0.5), log(qty)))
  holidayData$logPrice <- log(holidayData$price)
  # For recalibration purposes,set trend to 1
  holidayData$trend <- 1

  keyVar <- "mdse_item_i"
  useShareModel <- TRUE
  useRecalValues = modelData$useRecalValues
  recalCoeffs <- createCoeffs(recalData, holidayData, coeffs, promoVars, priceVar = priceVar, keyVar, holidaysVar, useShareModel, useRecalValues)
  # Need to choose coefficients based on whether we should use recent weeks or weekly data.  We choose based on values of useRecalValues
  itemLevelCoeffs <- chooseCoefficients(recalCoeffs, recalData, useRecalValues)
  itemLevelCoeffs <- data.frame(itemLevelCoeffs)
  # We pass holidays to the output because in production, we may not save modelData
  # We could have  extra holiday variables with .x and .y.  If so, drop them
  endsWith = function(x, suffix) {
    if (!is.character(x) || !is.character(suffix))
      stop("non-character object(s)")
    n = nchar(x)
    suppressWarnings(substr(x, n - nchar(suffix) + 1L, n) == suffix)
  }
  dropIndexX <- endsWith(names(itemLevelCoeffs), ".x")
  itemLevelCoeffs <- itemLevelCoeffs[!dropIndexX]
  dropIndexY <- endsWith(names(itemLevelCoeffs), ".y")
  itemLevelCoeffs <- itemLevelCoeffs[!dropIndexY]
  return(list(shareData = trainingData, shareCoeffs = itemLevelCoeffs, recalData = recalData, itemData = itemData, holidaysVar = holidaysVar))
}


chooseCoefficients <- function(recalCoeffs, recalData, useRecalValues){
  
  # For each sku, and each week, decide whether to see recent weeks or last year data for the intercept
  # Add the current recal values
  subsetRecalData <- subset(recalData$lastYearData, select = c("mdse_item_i", "wk_begin_date", "wk_end_date", "week", "qty", "holiday", "sales_storecount", "avg_ext_sls_prc_unit"))
  subsetUseRecalValues <- subset(useRecalValues, select = c("mdse_item_i", "useRecent"))
  combine <- merge(subsetRecalData, subsetUseRecalValues, by = c("mdse_item_i"))
  combine <- combine[order(combine$mdse_item_i, combine$week),]
  choices <- ddply(combine, .(mdse_item_i), decideWeeklyRecal)
  
  # It is possible that lastYear doesn't have values because we throw out certain records, so use the default values
  # But first, get the cross product of items and weeks
  weeks <- seq(1:52)
  grid <- merge(unique(combine$mdse_item_i), weeks)
  names(grid) <- c("mdse_item_i", "week")
  grid <- merge(grid, subsetUseRecalValues, by = c("mdse_item_i"))  
  choices <- merge(choices, grid, by = c("mdse_item_i", "week"), all.y = TRUE)
  choices$choice <- with(choices, ifelse(is.na(choice), ifelse(useRecent, "recent", "last_year"), choice))
  choices$useRecent <- NULL
  
  defaultCoeffs <- flattenCoefficients(recalCoeffs, useRecalValues)
  lastYearCoeffs <- defaultCoeffs$lastYearIntercepts
  recentCoeffs <- defaultCoeffs$recentIntercepts
  lastYearCoeffs2 <- merge(choices, lastYearCoeffs, by = c("mdse_item_i", "week"))
  lastYearCoeffs2 <- subset(lastYearCoeffs2, choice == "last_year")
  recentCoeffs2 <- merge(choices, recentCoeffs, by = c("mdse_item_i", "week"))
  recentCoeffs2 <- subset(recentCoeffs2, choice == "recent")
  combineCoeffs <- rbind(lastYearCoeffs2, recentCoeffs2)
  combineCoeffs$choice <- with(combineCoeffs, ifelse(is.na(choice), useRecent, choice))
  combineCoeffs$useRecent <- NULL
  combineCoeffs <- combineCoeffs[order(combineCoeffs$mdse_item_i, combineCoeffs$week),]
  stopifnot(nrow(combineCoeffs) == length(unique(combineCoeffs$mdse_item_i))*52)
  return(combineCoeffs)
}

# Get the weekly intercepts base on the first 13 weeks only because we only have 65 weeks of
# data. Starting on week 14, use the overall choice
decideWeeklyRecal <- function(input){

  value <- as.numeric(input[1, c("mdse_item_i")])
  debugMsg <- paste0("Granular weekly intercepts for ", value)
  #show(debugMsg)
  # Rekey
  input$defaultValue <- with(input, ifelse(useRecent == TRUE |is.na(useRecent), "recent", "last_year"))
  defaultValue <- as.character(input[1, c("defaultValue")])
  minWeek <- min(input$week) 
  input$newWeek <- input$week - minWeek + 1
  newMaxWeek <- max(input$newWeek)
  finalResults <- data.frame(week = integer(), choice = character())
  for (i in 1:13){
    rollingRecords <- subset(input, newWeek >= i & newWeek <=  i + 2 & holiday == "no_holiday")
    lastRecords <- subset(input, newWeek >= newMaxWeek - 2 & holiday == "no_holiday")
    if(nrow(rollingRecords) == 0){
      # Automatically use default if last year isn't available
      temp <- data.frame(week = i, choice = defaultValue)
      finalResults <- rbind(finalResults, temp)          
    }else{
      choice <- tryCatch({
        # Should I use qty or sales_storecount
        ks <- t.test(rollingRecords$qty/rollingRecords$sales_storecount, lastRecords$qty/lastRecords$sales_storecount)
        ks2 <- t.test(rollingRecords$avg_ext_sls_prc_unit, lastRecords$avg_ext_sls_prc_unit)
        if(is.na(ks$p.value) | is.na(ks2$p.value)){
          choice = defaultValue
        }else if( (!is.na(ks$p.value) & ks$p.value < 0.20) & (!is.na(ks2$p.value) & ks2$p.value < 0.20)) {
          choice = "recent"
        }else{
          choice = "last_year"
        }
      }, error = function(error){
        errorMsg <- paste0("Can't get values for  ", value, " and week = ", i)
        show(errorMsg)
        choice = defaultValue
      })
      temp <- data.frame(week = i, choice = choice )
      finalResults <- rbind(finalResults, temp)
    }
  }
  
  # For weeks 14-52, just select the input
  restOfTime <- subset(input, newWeek >= 14, select = c("newWeek", "defaultValue"))
  names(restOfTime) <- c("week", "choice")  
  finalResults <- rbind(finalResults, restOfTime)
  finalResults <- finalResults[order(finalResults$week),]
  return(finalResults)
}

flattenCoefficients <- function(recalCoeffs, useRecalValues){
  
  # Flatten out the skus for the 52 week and recent weeks
  #recentSkus <- subset(useRecalValues, useRecent == TRUE |is.na(useRecent), select = c("mdse_item_i"))
  #lastYearSkus <- subset(useRecalValues, useRecent == FALSE, select = c("mdse_item_i"))
  
  # Overwrite with everything but keep old code above just in case
  recentSkus <- subset(useRecalValues, select = c("mdse_item_i"))
  lastYearSkus <- subset(useRecalValues, select = c("mdse_item_i"))
  
  recentCoeffs <- merge(recalCoeffs$recent, recentSkus, by = c("mdse_item_i"))
  lastYearCoeffs <- merge(recalCoeffs$weekly, lastYearSkus, by = c("mdse_item_i"))

  # Flatten the coefficients for 52 weeks data
  recalNames <- paste0("recal_intercept_", seq(1:52))
  transposedLYC <- melt(lastYearCoeffs[, c("mdse_item_i", recalNames)], id  = c("mdse_item_i"))
  transposedLYC$week <- as.integer(gsub("recal_intercept_", "", transposedLYC$variable))
  transposedLYC$recal_intercept <- transposedLYC$value  
  transposedLYC$variable <- NULL  
  transposedLYC$value <- NULL  
  startsWith2 <- function(x){
      grepl("^recal_intercept_", x)
  }
  interceptsIndices <- !(startsWith2(names(lastYearCoeffs)))
  coeffsWithoutIntercepts <- lastYearCoeffs[, c(interceptsIndices)]
  transposedLYC2 <- merge(coeffsWithoutIntercepts, transposedLYC, by = c("mdse_item_i"))
  
  # Now blow out those with the same intercept each week
  weeks <- data.frame(week = seq(1:52))
  weeks$flag <- 1
  
  if(nrow(recentCoeffs) > 0){
    recentCoeffs$flag <- 1
    recentCoeffs2 <- merge(recentCoeffs, weeks, by = c("flag"))
  } else{
    recentCoeffs2 <- recentCoeffs
  }

  recentCoeffs2$flag <- NULL
  recentCoeffs2$X.Intercept. <- NULL
  recentCoeffs2$recal_intercept1 <- NULL
  recentCoeffs2$recal_intercept2 <- NULL
  recentCoeffs2$V1 <- NULL
  
  transposedLYC2$flag <- NULL
  transposedLYC2$X.Intercept. <- NULL
  transposedLYC2$recal_intercept1 <- NULL
  transposedLYC2$recal_intercept2 <- NULL
  transposedLYC2$V1 <- NULL
  
  return(list(recentIntercepts = recentCoeffs2, lastYearIntercepts = transposedLYC2))
}


getPredictedQty <- function(data, inputCoeffs, sharePromoVars, holidaysVar){
  
  inputCoeffs <- data.frame(inputCoeffs)

  data <- reorderDataByColName(data)
  inputCoeffs <- reorderDataByColName(inputCoeffs)

  # Create plots of predicted and actuals.
  #TODO: We shouldn't be hardcoding this
  assessData <- merge(data, inputCoeffs, by = c("mdse_item_i", "week"))
  assessData <- data.table(assessData)
  # Assume for now we always have price, seas, and trend.  
  assessData$predictQtyBase<- with(assessData, recal_intercept + logPrice.x*logPrice.y + seas.x*seas.y + trend.x*trend.y)
  # Add promo variables to predictQty
  dataIndices <- which(names(assessData) %in% paste0(sharePromoVars, ".x"), arr.ind = TRUE)
  coeffsIndices <- which(names(assessData) %in% paste0(sharePromoVars, ".y"), arr.ind = TRUE)
  keepData <- data.frame(apply(assessData[, c(dataIndices), with = FALSE]*assessData[, c(coeffsIndices), with = FALSE], 1, sum))
  names(keepData) <- c("promoVarSum")
  assessData$promoVarSum <- keepData$promoVarSum
  assessData$predictQtyBase <- assessData$predictQtyBase + keepData$promoVarSum 
  # Add the holiday data to predictQty
  dataIndices <- which(names(assessData) %in% paste0(holidaysVar, ".x"), arr.ind = TRUE)
  coeffsIndices <- which(names(assessData) %in% paste0(holidaysVar, ".y"), arr.ind = TRUE)
  keepData <- data.frame(apply(assessData[, c(dataIndices), with = FALSE]*assessData[, c(coeffsIndices), with = FALSE], 1, sum))
  names(keepData) <- c("holVarSum")
  assessData$predictQty <- exp(assessData$predictQtyBase + keepData$holVarSum) 
  assessData$holVarSum <- keepData$holVarSum
  
  return(assessData)
  
}

assessShareModel <- function(data, coeffs, sharePromoVars, holidaysVar, plotGraphs = TRUE){
  
  assessData <- getPredictedQty(data, coeffs, sharePromoVars, holidaysVar, "date")
  assessData <- assessData[order(assessData$mdse_item_i, assessData$week),]
  # Take care of qty == 0 by adding 1
  assessData$qty <- with(assessData, ifelse(is.na(qty), 0, qty))
  assessData$mape <- with(assessData, (predictQty-qty)/(qty+1))
  assessData$absmape <- abs(assessData$mape)
  #blah <- subset(assessData, mdse_item_i == 3555100 | mdse_item_i == 5432569)
  
  test <- assessData
  test <- data.frame(test)
  
  if(plotGraphs){
  # There is a better way to do this than looping.  I can't get the darn facet_grid to work
  for (value in unique(test$mdse_item_i)){
    debugMsg <- paste0("Processing ", value)
    show(debugMsg)
    plotData <- subset(test, mdse_item_i == value)
    if(nrow(plotData) > 1){
    myPlot <- ggplot(plotData, aes(date, group = mdse_item_i, color = variable)) +
      geom_line(aes(y = qty, col = "qty")) +
      geom_line(aes(y = predictQty, col = "predQty"))
    myPlot <- myPlot + ggtitle(value)
    print(myPlot)
    }	
  }
  
  # Get rid of the baseunits for now
  #test <- subset(assessData, select = c("mdse_item_i", "week", "date", "qty", "predictQty", "mape", "absmape", "baseunits", "predictBaseunits", "mapeBaseunits", "absmapeBaseunits"))
  test <- subset(assessData, select = c("mdse_item_i", "week", "date", "qty", "predictQty", "mape", "absmape"))
  
  
  mape1 <- data.frame(do.call("rbind", tapply(assessData$mape, assessData$mdse_item_i, quantile, seq(0,1, by = 0.2))))
  mape1$mdse_item_i <- row.names(mape1)
  mape2 <- data.frame(do.call("rbind", tapply(assessData$absmape, assessData$mdse_item_i, quantile, seq(0,1, by = 0.2))))
  mape2$mdse_item_i <- row.names(mape2)
  show("Quantiles of MAPE for training data")
  show(mape1)
  #show(quantile(mape1$X50.))
  show("Quantiles of Absolute MAPE for training data")
  show(mape2)
  #show(quantile(mape2$X50.))
  }
  return(assessData)
}

