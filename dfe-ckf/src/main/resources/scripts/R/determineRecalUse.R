# Determine recalibration period for each item by assigning
# FALSE if you want to use last year, and TRUE otherwise
# Input should be for one item
determineRecalUse <- function(input, endOfTraining){

  input <- subset(input, select = c("wk_end_date", "mdse_item_i", "sls_unit_q", "reg_retl_a"))
  #  input <- subset(salesData, mdse_item_i == problemSku, select = c("wk_end_date", "mdse_item_i", "sls_unit_q", "reg_retl_a"))
  firstDate <- as.Date(endOfTraining) - 7*130 + 1
  weeklyMapping <- getWeeklyMapping(firstDate, endOfTraining)
  weeklyMapping <- aggregate(date ~ week, data = weeklyMapping, max)
  names(weeklyMapping) <- c("week", "wk_end_date")
  
  input <- merge(input, weeklyMapping, by = c("wk_end_date"), all.y = TRUE)  
  # Should I set missing to zero for comparison sake?
  #input[is.na(input$sls_unit_q)]$sls_unit_q <- 0
  # Compare this year's current quarter to last year's previous quarter
  maxWeek <- max(input$week)
  startCurrentQuarter <- maxWeek - 13
  endWeekSameTimeLastYear <- maxWeek - 52
  startWeekSameTimeLastYear <- endWeekSameTimeLastYear - 13
  values1 <- subset(input, week <= endWeekSameTimeLastYear & week >= startWeekSameTimeLastYear, select  = c("sls_unit_q", "week", "wk_end_date", "reg_retl_a"))
  values2 <- subset(input, week <= maxWeek & week >= startCurrentQuarter, select = c("sls_unit_q", "week", "wk_end_date", "reg_retl_a"))

  # Divide the data into halves and see if the two halves are statistically significant
  if(sum(values1$sls_unit_q, na.rm = TRUE) == 0 | sum(values2$sls_unit_q, na.rm = TRUE) == 0){
    returnDF <- cbind.data.frame(TRUE, NA, NA)
    names(returnDF) <- c("useRecent", "qtyUseRecent", "priceUseRecent")
    return(returnDF)
  }
  values1$flag <- 0
  values1[!is.na(values1$sls_unit_q),]$flag <- 1
  values2$flag <- 0
  values2[!is.na(values2$sls_unit_q),]$flag <- 1
  sum1 <- sum(values1$flag, na.rm = TRUE)
  sum2 <- sum(values2$flag, na.rm = TRUE)
  
  # Default to recent weeks when you don't have a lot of data
  qtyReturnValue <- TRUE
  if(sum1 > 2 & sum2 > 2){
      length1 <- sum(!is.na(values1$sls_unit_q))
      length2 <- sum(!is.na(values2$sls_unit_q))
      # If the first series is longer than second by 3 and if it wasn't missing last year but is missing this
      # year, fill in the missing with zeros
      if(length1 - length2 >= 3){
          values2[!is.na(values1$sls_unit_q) & is.na(values2$sls_unit_q),]$sls_unit_q <- 0      
      }
      ts <- t.test(values1$sls_unit_q,values2$sls_unit_q)
      ks <- ks.test(values1$sls_unit_q, values2$sls_unit_q)
      if(!is.na(ks$p.value) & ks$p.value < 0.05 | !is.na(ts$p.value) & ts$p.value < 0.05){
        qtyReturnValue <- TRUE
      } else{
        qtyReturnValue <- FALSE
      }
  } 

  # Need to test prices too (see item 9268652.  Its price dropped a lot, although its units did not.  $2.75 in early Jan 2016.  By the
  # end of the year, it was closer to $1.00. Intercept recalibration assumes relatively stable inputs)  
  ks <- ks.test(values1$reg_retl_a, values2$reg_retl_a)
  if(!is.na(ks$p.value) & ks$p.value < 0.05){
    priceReturnValue <- TRUE
  } else{
    priceReturnValue <- FALSE
  }
  
  if(qtyReturnValue == TRUE){
    finalValue <- TRUE
  }else{
    finalValue <- FALSE
  }
  returnDF <- cbind.data.frame(finalValue, qtyReturnValue, priceReturnValue, mean(values1$sls_unit_q, na.rm = TRUE), mean(values2$sls_unit_q, na.rm = TRUE))
  names(returnDF) <- c("useRecent", "qtyUseRecent", "priceUseRecent", "sales_lastyear", "sales_thisyear")
  return(returnDF)
}

