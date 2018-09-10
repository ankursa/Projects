require(plyr)
require(dplyr)
require(data.table)
require(MCMCpack)
source("modelingUtil.R")

createItemData <- function(input, promoVars, holidaysVar){
  
  input <- data.table(input)
  # We already have data at store level.  Don't need to aggregate it again
  # Below, we need to name the variable "qty" to make it consistent with existing field names in the input
  weightedVars <- c("price", "baseprice")
  nonWeightedVars <- c(promoVars, "seas", "avg_ext_sls_prc_unit", "promoPrice", holidaysVar)

  getMeanData <- function(input, aggKeys, weightedVars, nonWeightedVars){
    aggKeys <- c(aggKeys, "date")
    weightedMeanVars <- input[!is.na(qty),  lapply(mget(weightedVars), weighted.mean, na.rm = TRUE, w = qty), by = c(aggKeys)]
    #meanVars <- input[,  lapply(.SD, mean), .SDcols = c(vars), by = c(aggKeys, "date")]
    nonWeightedMeanVars <- input[,  lapply(.SD, mean, na.rm = TRUE), .SDcols = c(nonWeightedVars), by = c(aggKeys)]
    meanVars <- merge(weightedMeanVars, nonWeightedMeanVars, by = aggKeys)
    sumVars <- input[, .(qty = sum(qty, na.rm = TRUE), daysOfZeroSales = sum(daysOfZeroSales), baseunits = sum(baseunits, na.rm = TRUE)), by = aggKeys]
    output <- merge(meanVars, sumVars, by = aggKeys)    
    
    # Throw out zero records because we are dealing with aggregated data and they shouldn't be zero often, particulary
    # when we are aggregating up to a zone level or zone-subgroup level
    # Yes, this will lead to a bias in the model but hopefully, we won't need
    # to care about low volume products
    output <- subset(output, qty > 0)
  }
  
  # Create several datasets based on different levels of aggregation (ie entire category, item level, group level)
  aggKeys <- c("subgroup", "mdse_item_i", "week", "holiday")
  itemData <- getMeanData(input, aggKeys, weightedVars, nonWeightedVars)
  itemData$co_loc_i <- -1
  
  # Now aggregate up to subgroup-chain level.  Add holiday to the website
  aggKeys <- c("subgroup", "week", "holiday")
  subgroupData <- getMeanData(input, aggKeys, weightedVars, nonWeightedVars)
  subgroupData$mdse_item_i <- subgroupData$subgroup
  subgroupData$co_loc_i <- -1     
  
  # Create another level of aggregation.  Create a level so that fixed effect is at category, random at subgroup level
  subgroupData2 <- subgroupData
  subgroupData2$subgroup <- "SUBGROUPS"
  
  # Now aggregate up to the category-chain level
  aggKeys <- c("week", "holiday")
  allData <- getMeanData(input, aggKeys, weightedVars, nonWeightedVars)
  allData$mdse_item_i <- "ALL"
  allData$subgroup <- "SUBGROUPS"
  allData$co_loc_i <- -1  
  
  allData2 <- allData
  allData2$subgroup <- "CATEGORY"
  
  # Flag anomalous records from training data.  
  # example: mdse_item_i == 4971793 & week %in% c(60, 61, 62)), week = 62 looks odd
  # blah2 <- subset(trainingData, mdse_item_i == 4971793)
  # example 2:HAM_GOOD, 293954, week = 42 with a value of 8 is not an outlier? Nope, high variance in qty
  # At the store level with promotions with few variation in the promo, any non-zero value gets flagged.  
  # We don't want to throw those records out.  In aggregate, it may be different
  # Given that promotion variables with few variation get flagged, use only discount to reflect promotions for now
  # We toss certain records under the assumption that at high level of aggregations should have a minimum number of units 
  # and if it is low, it is due to inventory or lifecycle, neither of which we are accounting for now.  Instead, we will
  # throw the records out  (eg.10175659 has a high positive coefficient unless we exclude the low units)
  #varsToConsider <- c("discount")
  varsToConsider <- c(holidaysVar, "circular")
  varsOfInterest <- c("qty", "daysOfZeroSales")
  allData2_clean <- ddply(allData2, .(allData2$subgroup), function(x){identifyOutliers(x, varsOfInterest, varsToConsider)})
  allData2_clean <- allData2_clean[-c(1)]
  allData2_clean <- data.table(allData2_clean)
  allData_clean <- ddply(allData, .(allData$subgroup), function(x){identifyOutliers(x, varsOfInterest, varsToConsider)})
  allData_clean <- allData_clean[-c(1)]
  allData_clean <- data.table(allData_clean)
  subgroupData_clean <- ddply(subgroupData, .(subgroupData$subgroup), function(x){identifyOutliers(x, varsOfInterest, varsToConsider)})
  subgroupData_clean <- subgroupData_clean[-c(1)]
  subgroupData_clean <- data.table(subgroupData_clean)
  subgroupData2_clean <- ddply(subgroupData2, .(subgroupData2$mdse_item_i), function(x){identifyOutliers(x, varsOfInterest, varsToConsider)})
  subgroupData2_clean <- subgroupData2_clean[-c(1)]
  subgroupData2_clean <- data.table(subgroupData2_clean)
  itemData_clean <- ddply(itemData, .(itemData$mdse_item_i), function(x){identifyOutliers(x, varsOfInterest, varsToConsider)})
  itemData_clean <- itemData_clean[-c(1)]
  itemData_clean <- data.table(itemData_clean)
  
  # Create another category 
  # Concat them together for now
  # For now, don't run on store-level data
  # Where is week 3 for mdse_item_i == 4971793 in blah2, where blah2 = subset(trainingData, mdse_item_i == 4971793): Answer, qty = 0 at week level

  l <- list(allData2_clean, allData_clean, subgroupData2_clean, subgroupData_clean, itemData_clean)
  trainingData <- rbindlist(l,  fill = TRUE)
  return(trainingData)
}




