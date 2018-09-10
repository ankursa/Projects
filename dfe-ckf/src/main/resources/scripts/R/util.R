require(MASS)


# Utility functions

saveForScoring <- function(outputLoc, listOfObjects, key){
  # Need to save outputs needed from training for scoring
  objectNames <- ls(listOfObjects)
  fileName <- paste0("listOfObjects_", key, ".rds")
  hadoopFile <- paste0(outputLoc, "/", fileName)
  saveRDS(listOfObjects, fileName)
  system(paste0("hdfs dfs -copyFromLocal -f ",  fileName , " ", hadoopFile))

  # Is it better to save the individual objects or just save the list?  Maybe just save the list of now?
  if(FALSE){
  for(i in 1:length(objectNames)){
      current <- objectNames[i]
      fileName <- paste0(current, ".rds")
      saveRDS(current, fileName)
      hadoopFile <- paste0(outputLoc, "/", fileName)
      system(paste0("hdfs dfs -copyFromLocal -f ",  fileName , " ", hadoopFile))
  }
  }
}

# Create a weekly mapping in such a way that the last day - 7 defines the last
# week.  This means we can have non-Sunday to Saturday chunk (ie Friday to Thursday are allowed)
# firstDate = first date of the available data
# lastDate = last date of all available data
# return a dataset with variables "date" and "week", where each date is mapped to a week
getWeeklyMapping <-function(firstDate, lastDate){
  
  dates <- data.frame(seq(from = firstDate, to = lastDate, by = 1))
  names(dates) <- c("date")
  calendar <- data.frame(dates[order(dates$date, decreasing = TRUE),])
  names(calendar) <- c("date")
  
  # Create counter
  calendar$counter <- seq(1:nrow(calendar))
  
  # Group the data
  calendar$group <- ceiling(calendar$counter/7)
  
  # This was done in reverse order so we now need to reverse the weeks so that cal_weeknr = 1 is the first week
  maxValue <- max(calendar$group)
  calendar$week <- maxValue - calendar$group + 1
  calendar$group <- NULL
  calendar$counter <- NULL
  calendar <- calendar[order(calendar$date),]
  
  return(calendar)
}

# Function to get the mode of a particular vector
getMode <- function(v) {
  uniqv <- unique(v)
  uniqv[which.max(tabulate(match(v, uniqv)))]
}

# Flags outliers for qty only if other causal variables are also not considered outliers
# For example, if qty is very high, maybe it is because discount is very high.  We would want to
# keep that.  Consequently, varsToConsider should include discount.
# Testing shows that discount doesn't always correlate to high sales.  We will
# keep the code, but set the varsToConsider to be empty for now
# This method adds a new variable called "outlier" to the dataset
identifyOutliers <- function(input, varsOfInterest = c("qty", "daysOfZeroSales"), varsToConsider = NULL){
  # Helper method to get the indices for flagged results for a given variable
  getIndices <- function(input, var){
    input <- data.table(input)
    y <- input[, c(var), with = FALSE]  
    hy<-hubers(y) 
    hscore<-(y-hy$mu)/hy$s 
    # Have different cut-offs for different variables
    # If the variable is daysOfZeroSales, have a tighter range
    # How did I choose these ranges?  I looked at the data and tried some
    # back-of-the-envelop where throwing out records in certain ranges
    # materially changed the regression.  
    # TODO: Find the cutoffs in a less ad-hoc way
    cutoff <- 2.5
    if(var == "daysOfZeroSales")
      cutoff <- 1.5
    else if (var == "discount")
      cutoff <- 3.5
    # We want the negative values too.  
    index <- which(hscore >cutoff | hscore <= -1.35)
    return(index)
  }
  
  # The idea is to flag outlying quantities by identifying
  # and intersecting them with outlying causals, defined by varsToConsider
  # If the varsToConsider don't explain the outlying qty records, then label 
  # the point as an outlier
  outlierIndices <- NULL
  for(i in 1:length(varsOfInterest)){
    mainIndices <- getIndices(input, varsOfInterest[i])
    if (i == 1){
      outlierIndices <- mainIndices
    } else{
      outlierIndices <- union(mainIndices, outlierIndices)
    }
  }        
  indicesStore <- list()
  if(length(outlierIndices) > 0 & length(varsToConsider) > 0){
    for(i in 1:length(varsToConsider)){
      indicesStore <- c(indicesStore, getIndices(input, varsToConsider[i]))
      outlierIndices <- setdiff(outlierIndices, indicesStore)
    }
  }
  input$outlier <- 0
  if(length(outlierIndices) > 0)
    input$outlier[outlierIndices] <- 1
  return(input)
}

# Another algorithm for identifying outliers. X = series, k = span around data point, t0 is equivalent of +/- SD
HampelFilter <- function (x, k,t0=1){
  n <- length(x)
  y <- x
  ind <- c()
  L <- 1.4826
  for (i in (k + 1):(n - k)) {
    x0 <- median(x[(i - k):(i + k)], na.rm = TRUE)
    S0 <- L * median(abs(x[(i - k):(i + k)] - x0))
    if (abs(x[i] - x0) > t0 * S0) {
      y[i] <- x0
      ind <- c(ind, i)
    }
  }
  list(y = y, ind = ind)
}

# Count the number of times a variable has the value of 1 and returns a dataset with the variable "run", indicating the number
# of consecutive "1" values.  "Run" will be populated with the value of consecutive "1", starting from the beginning of the consecutive series
# to the end of the consecutive series
calcRun <- function(input, var = "outlier"){
  input <- data.frame(input)
  outlier <- as.numeric(input[,c(var)])
  run <- NULL
  for(i in 1:nrow(input)){
    if(outlier[i] == 1){
      if(i == 1){
        run[i] <- 1 
      }else{ 
        value <- run[i-1] + outlier[i]
        #debugMsg <- paste0("i = ", i, " value = ", value)
        #show(debugMsg)
        run[(i-value+1):(i)] <- value
      }
    } else run[i] <- 0
  }
  input <- cbind.data.frame(input, run)
  return(input)
}

# Given a date, assign a quarter
assignQuarter <- function(input, dateVar = "date"){
  input <- data.frame(input)
  quarter <- ifelse(month(input[,c(dateVar)]) <=3, 1, ifelse(month(input[,c(dateVar)]) <= 6, 2, ifelse(month(input[,c(dateVar)]) <= 9, 3, 4)))
}


# Compare last years to forecast to this year via plots
compareMe <- function(past, future){
  
    # Need to rekey the weeks as last week may not be week 52.
    past <- shareModel$itemData
    lastWeek <- max(past$week)
    past$origWeek <- past$week
    past$week <- lastWeek - past$origWeek + 1
    future <- forecasts$prodPreds
    # Need to add week to the dataset for merging purposes
    future <- future[order(future$mdse_item_i, future$date),]
    future <- data.table(future)
    future <- future[, week := seq(1, .N), by = c("mdse_item_i")]
    past$pastDate <- past$date
    past$mdse_item_i <- as.numeric(past$mdse_item_i)
    # Replace qty with the past qty
    newData <- merge(past[, c("week", "mdse_item_i", "qty", "pastDate")], future, by = c("mdse_item_i", "week"))
    
    # I am re-using code.  Maybe this should be a function?  Yeah, probably -- whatever
    test <- newData
    # There is a better way to do this than looping.  I can't get the darn facet_grid to work
    for (value in unique(test$mdse_item_i)){
      debugMsg <- paste0("Processing ", value)
      show(debugMsg)
      plotData <- subset(test, mdse_item_i == value)
      if(nrow(plotData) > 1){
        myPlot <- ggplot(plotData, aes(date, group = mdse_item_i, color = variable)) +
          geom_line(aes(y = qty, col = "qty")) +
          geom_line(aes(y = forecast_q, col = "predictQty"))
        myPlot <- myPlot + ggtitle(value)
        print(myPlot)
      }	
    }
    
    return(newData)
}



