require(data.table)
require(dplyr)
source("util.R")


# Given a dataset a record, determines whether or not the record is an outlier using the X records around it, where X is 
# denoted by variable span.  The number of records looks 'span' records forward.  If we reach the end before the number of
# 'span' records is reached, we grab the prior records until we get the X values.  We assume that the number of records
# is greater than 'span'.
# After determining the outlier records, it counts the consecutive run of outliers.  If the consecutive run is more than 8
# (configured by runThreshold), we throw out all data prior to that streak and leave the rest.  If the streak of consecutive 
# run happens within the 8 weeks of the last week of training (configured by windowSize), 
# we don't throw it out because we assume we are in a new regime and go to the previous qualifying streak, if it exists.
# Var represents the variable to use to determine if it an outlier 
# Span represents the number of points to evaluate around the record of interest
# Maxweek indicates the number of weeks in the data.  
cleanSeriesWorker <- function(input, numTrainingWeeks, var = "qty", span = 8, runThreshold = 5, windowSize = 5){
  n <- nrow(input)
  outliers <- NULL
  input <- data.frame(input)
  x <- as.numeric(input[, c(var)])
  for (i in 1:n){
    # test will store the next X values going forward.  If we reach the end before X is reached,
    # we grab the prior records until we get X values
    test <- NULL
    if(i <= (n - span)){
      test <- x[(i + 1):(i + span)]
    }else if(i==n){
      test <- x[(i-span):(n-1)]
    }else{
      test <- dplyr::combine(x[(i-(i+span-n)):(i-1)], x[(i + 1):(n)])
    }  
    newTest <- c(x[i], test)
    hy<-hubers(newTest) 
    hscore<-(newTest-hy$mu)/hy$s 
    outliers[i] <- 0
    if(is.na(hscore[1]) | is.nan(hscore[1]) | is.infinite(hscore[1])){
        if(x[i] <= 15 & test[span] <= 15){
          outliers[i] <- 0
        }
    }else if(hscore[1] > 1.85 | hscore[1] < -1.85){
      outliers[i] <- 1
    }else {
      diffScore <- abs(hscore[1] - mean(hscore[(span-1):(span+1)], na.rm = TRUE))
      ratio <-  (x[i]/mean(test[(span-3):span], na.rm = TRUE))
      if(diffScore > 0.5 & (ratio > 1.3 | ratio < 0.7)){
        outliers[i] <- 1
      }
    }  
  }
  output <- cbind.data.frame(input, outliers)
  run <- calcRun(output, var = "outliers")
  runValues <- sort(unique(run$run))
  runValues <- runValues[runValues > runThreshold]
  # Starting with the longest run length, purge everything before that date, if it is outside the window.  If it is inside
  # the window, go to any previous, qualifying length window and if it is outside the window, purge up to then
  if(length(runValues) == 0)
    return(run)
  
  for(i in length(runValues):1){
    value <- runValues[i]
    temp <- subset(run, run == value)
    maxWeekRun <- max(temp$week)
    if(numTrainingWeeks - windowSize >= maxWeekRun){
      # Purge everything up to that point but only if we have at least 8 datapoint.  If we don't have enough
      # data, purge everything up to 8 data points
      temp2 <- subset(run, week > maxWeekRun)
      if(nrow(temp2) >= 8){
        run <- temp2
        break;
      }else{
         extra <- 8 - nrow(temp2)
         temp2 <- subset(run, week > maxWeekRun - extra)
         run <- temp2
         break;
      }
    }      
  }
  return(run)
}

# Identify series where sales are very different for a short period of time
#18389207
#8807214
#10130146
#16077800

cleanSeries <- function(origSeries, numTrainingWeeks, var = "qty", span = 8, runThreshold = 5, windowSize = 5, holidaysVar){
  
  holidaysVarExist <- FALSE
  if(!is.null(holidaysVar) | length(holidaysVar) > 0){
    analysisSeries <- data.frame(origSeries)
    analysisSeries$totalHoliday <- rowSums(analysisSeries[,holidaysVar])
    # Don't include records that are high or lower because of holidays
    holidaySeries <- subset(analysisSeries, totalHoliday == 1)
    origSeries <- subset(analysisSeries, totalHoliday == 0)
    origSeries$totalHoliday <- NULL
    holidaySeries$totalHoliday <- NULL
    analysisSeries$totalHoliday <- NULL
    holidaysVarExist <- TRUE
  }

  # The cleanSeriesWorker algorithm cannot take missing, so throw them out.  
  if(!holidaysVarExist){
      analysisSeries <- data.frame(origSeries)
  }
  if(holidaysVarExist){
    series <- subset(analysisSeries, select = c("date", "week", var, holidaysVar))
  }else{
    series <- subset(analysisSeries, select = c("date", "week", var))
  }
  series <- series[!is.na(series[, c(var)]),]
  series <- unique(series)
  if(nrow(series) < 10){
    return(origSeries)
  }
  if(holidaysVarExist){
    series <- identifyOutliers(series, varsOfInterest = c(var), varsToConsider = c(holidaysVar))
  }else{
    series <- identifyOutliers(series, varsOfInterest = c(var), varsToConsider = NULL)
  }
  # It is possible that the outliers are in the last weeks of the training set.  If so, don't remove them, as it may represent we are in a new
  # regime (eg. 10130146 up to 4-30-2016).  Count the number of outliers.  If there is a series of them and they reside
  # close to the end of the training period, then don't count them as an outlier.  We will use the records in 
  # cleanSeriesWorker to identify outliers using a neigborhood
  series <- calcRun(series)
  series$new_outlier <- with(series, ifelse(week > numTrainingWeeks - 13 & run >= 3, 0, outlier))
  input <- subset(series, new_outlier == 0, select = c(var, "week"))
  if(nrow(input) < 10){
    series3 <- merge(analysisSeries, input[c("week")], by = c("week"))
    return (series3)
  }
  series2 <- cleanSeriesWorker(input, numTrainingWeeks = numTrainingWeeks, var, span, runThreshold, windowSize)
  series3 <- merge(analysisSeries, series2[c("week")], by = c("week"))
  series3 <- unique(series3)
  return(series3)  
}

