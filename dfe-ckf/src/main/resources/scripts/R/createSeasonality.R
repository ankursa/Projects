# Creates seasonality at weekly level. It removes records at promoted records at daily levels before aggregating up
require("plyr")
source("seasonality.R")

createSeasonality <- function(baseunits, datesDS){
  
  numTrainingWeeks <- nrow(datesDS)
  baseunits <- data.table(baseunits)
  cleanBaseunits <- ddply(baseunits, c("mdse_item_i"), function(x){cleanSeries(x, numTrainingWeeks, var = "baseunits", holidaysVar = NULL)})
  seas_window <- 4
  seasInputs <- createSeasonalityInputs(baseunits)
  finalSeasInputs <- checkSeasInputLength(seasInputs, baseunits, datesDS)
  seasOutput <- ddply(finalSeasInputs, .(finalSeasInputs$subgroup), function(x) {seasonality(x, seas_window)})
  names(seasOutput)[1] <- c("subgroup")
  seasOutput$subgroup <- as.character(seasOutput$subgroup)
  seasOutput[, c("subgroup", "week", "date", "baseunits", "seas", "qty")]

}

# If a subgroup doesn't have enough data, replace the data with the ones from the category level
checkSeasInputLength <- function(seasInputs, baseunits, datesDS){
  checkSum <- sum(seasInputs$baseunits, na.rm = TRUE)
  if(!is.na(checkSum) & checkSum > 0){
    seasInputsStats <- seasInputs[baseunits > 0 & !is.na(baseunits), .(count = .N), by = c("subgroup")]
    seasInputs <- merge(seasInputsStats, seasInputs, by = c("subgroup"), all.y = TRUE)
    # We want at least 65 weeks of data.  It would be ideal to have two years, but seeing how we want the first quarter
    # after end of training to be most accurate, we will settle for 1.25 years worth of data
    goodData <- subset(seasInputs, count >=65 & subgroup != "CATEGORY")
    badSubgroups <- subset(seasInputs, is.na(count) | count < 65 & subgroup != "CATEGORY", select = c("subgroup"))
    badSubgroups <- unique(badSubgroups$subgroup)
    categoryData <- subset(seasInputs, subgroup == "CATEGORY")
    if(nrow(categoryData) <=65){
      newCategoryData <- bootstrapCategoryDataBaseunits(categoryData, datesDS)
    }else{
      newCategoryData <- categoryData
    }
    if(length(badSubgroups) > 0){
      seasDataForBadSubgroups <- NULL
      for(i in 1:length(badSubgroups)){
        tempData <- newCategoryData    
        tempData$subgroup <- badSubgroups[i]
        if(i == 1){
          seasDataForBadSubgroups <- tempData   
        }else{
          seasDataForBadSubgroups <- rbind(seasDataForBadSubgroups, tempData)
        }
      }
      finalSeasInputs <- rbind(goodData, seasDataForBadSubgroups)
    }else{
      finalSeasInputs <- goodData
    }
    finalSeasInputs <- rbind(newCategoryData, finalSeasInputs)
  }else{
     newSeasInputs <- createSeasonalityInputs(baseunits)
     badSubgroups <- subset(seasInputs, subgroup != "CATEGORY", select = c("subgroup"))
     badSubgroups <- unique(badSubgroups$subgroup)
     categoryData <- subset(newSeasInputs, subgroup == "CATEGORY")
     newCategoryData <- bootstrapCategoryDataBaseunits(categoryData, datesDS)
     # If the category level is bad, then so is the subgroup level.  Borrow the category data for the subgroups too
     finalSeasInputs <- data.frame()
     for (i in 1:length(badSubgroups)){
        tempData <- newCategoryData
        tempData$subgroup <- badSubgroups[i] 
        finalSeasInputs <- rbind(finalSeasInputs, tempData)
     }
     finalSeasInputs <- rbind(newCategoryData, finalSeasInputs)
  }
  return(finalSeasInputs)
}

# If the category data is less than 65, just extend the data so that we get 52 weeks of data
bootstrapCategoryDataBaseunits <- function(input, datesDS){
  
  cleanCategoryData <- ddply(input, c("subgroup"), function(x){cleanSeries(x, nrow(datesDS), var = "baseunits", holidaysVar = NULL)})
  cleanCategoryData <- data.table(cleanCategoryData)
  input2 <- merge(cleanCategoryData, datesDS, by.x = c("week", "date"), by.y = c("week", "sls_d"), all.y = TRUE)
  input2$subgroup <- "CATEGORY"
  # Bootstrap the baseunits using the available data
  # Looping through each missing shouldn't be too bad since training tends to be short (130 weeks) and we are only doing
  # this for the category level
  for(i in 1:nrow(input2)){
    value <- input2[i,]$baseunits
    if(is.na(value)){
      newValue <- mean(sample(cleanCategoryData$baseunits, 10, replace = TRUE))
      input2[i,]$baseunits <- newValue
    }
  }
  
  return(input2)
}

plotSeasonality <- function(inputDS){
  
  for (value in unique(inputDS$subgroup)){
    debugMsg <- paste0("Processing ", value)
    show(debugMsg)
    plotData <- subset(inputDS, subgroup == value)
    myPlot <- ggplot(plotData, aes(date, group = subgroup, color = variable)) +
      geom_line(aes(y = seas, col = "seas"))
    myPlot <- myPlot + ggtitle(value)
    print(myPlot)
  }
}


plotBaseunitsAndQty <- function(inputDS){
  for (value in unique(inputDS$subgroup)){
    debugMsg <- paste0("Processing ", value)
    show(debugMsg)
    output <- subset(inputDS, subgroup == value)
    
    myPlot <- ggplot(output, aes(date, y = value, color = variable)) + 
      geom_line(aes(y = qty, col = "qty")) + 
      geom_line(aes(y = baseunits, col = "baseunits"))
    myPlot <- myPlot + ggtitle(value)
    print(myPlot)
  }
}


createSeasonalityInputs <- function(baseunitsDS){
    
  # Technically, we should use mean to account for different assortments in stores, but it doesn't seem to be working so well
  # because the quantities are so low
  
  aggSeasInputs <- baseunitsDS[, .(baseunits = mean(baseunits/sales_storecount, na.rm = TRUE),
                                    qty = sum(qty, na.rm = TRUE),
                                    date = max(date)), 
                                    by = .(subgroup, week)]  
  
  catSeasInputs <- baseunitsDS[, .(baseunits = mean(baseunits/sales_storecount, na.rm = TRUE),
                                   qty = sum(qty, na.rm = TRUE),
                                   date = max(date)), 
                               by = .(week)]  
  if(FALSE){
  baseunitsDS <- data.table(baseunitsDS)
  baseunitsDS <- baseunitsDS[!is.na(qty), item_count := .N, by = c("subgroup", "week")]
  baseunitsDS[is.na(baseunitsDS$item_count),]$item_count <- 1 

  aggSeasInputs <- baseunitsDS[, .(baseunits =  sum(baseunits, na.rm = TRUE),
                                   sales_storecount = mean(sales_storecount, na.rm = TRUE),
                                   item_count = mean(item_count, na.rm = TRUE),
                                   qty = sum(qty, na.rm = TRUE),
                                   date = max(date)), 
                               by = .(subgroup, week)]  
  aggSeasInputs[is.na(aggSeasInputs$sales_storecount),]$sales_storecount <- 1
  aggSeasInputs$baseunits <- with(aggSeasInputs, (baseunits/sales_storecount)/item_count)

  catSeasInputs <- baseunitsDS[, .(baseunits =  sum(baseunits, na.rm = TRUE),
                                   sales_storecount = mean(sales_storecount, na.rm = TRUE),
                                   item_count = mean(item_count, na.rm = TRUE),
                                   qty = sum(qty, na.rm = TRUE),
                                   date = max(date)), 
                               by = .(week)]  
  catSeasInputs[is.na(catSeasInputs$sales_storecount),]$sales_storecount <- 1
  catSeasInputs$baseunits <- with(catSeasInputs, (baseunits/sales_storecount)/item_count)
  }
  catSeasInputs$subgroup <- "CATEGORY"
  aggSeasInputs <- rbind(catSeasInputs, aggSeasInputs)
  
}

