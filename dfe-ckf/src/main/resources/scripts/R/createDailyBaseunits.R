require(plyr)
require(dplyr)
require(data.table)
source("make_calendar.R")
source("baseUnit.R")

createDailyBaseunits <- function(inputDS, subGroups, dayWeekMapping, baseunitsWindow = 8, promoVars = c("circular", "tpc", "peroff", "qdlr")){
  
  records <- removeRecords(inputDS, promoVars)
  baseunits <- records[, baseunits := createBaseunits(init_baseunits, baseunitsWindow), by = .(mdse_item_i, co_loc_i)] 
  baseunits <- baseunits[, baseunits := imputeBaseunits(baseunits), by = .(mdse_item_i, co_loc_i)]
  baseunits$baseunits <- with(baseunits, ifelse(baseunits > qty, qty, baseunits))
  baseunits$baseunits <- ceiling(baseunits$baseunits)
  baseunits <- data.frame(baseunits)
  baseunits2 <- merge(baseunits, subGroups[c("mdse_item_i", "subgroup")], by = c("mdse_item_i"))
  baseunits2 <- baseunits2[, c("mdse_item_i", "co_loc_i", "sls_d", "subgroup", "baseunits", "qty", "sales_storecount")]
  dayWeekMapping <- data.table(dayWeekMapping)
  baseunits2 <- merge(dayWeekMapping, baseunits2, by.x = c("date"), by.y = c("sls_d"))
  
}


# Remove discounted, promoted, and holiday records
removeRecords <- function(inputDS, promoVars){

  fogod <- data.table(inputDS)
  # Get holidays - do we need all the columns in calendar? maybe we can simplify the work in make_calendar?
  calendar <- make_calendar(c(as.character(min(fogod$sls_d)), as.character(max(fogod$sls_d))))
  #*******     TODO: Add memorial day
  # We only choose certain holidays and then flag those holidays for removal
  holidays <- calendar[,c("date", "year",  
#                          "laborday", "julyfourth", "thanksgiving","black.friday", "cyber.monday", "christmas.eve", 
                          "thanksgiving","black.friday", "cyber.monday", "christmas.eve", 
                          "christmas", "newyearsday", "newyearseve")]
  holidays <- data.table(holidays)
  holidays$holidayFlag <- 0
  holidays$holidayFlag[holidays$easter == 1 | holidays$julyfourth == 1 | holidays$halloween == 1 | holidays$valentines == 1
                       | holidays$laborday == 1 | holidays$memorialday == 1
               | holidays$christmas == 1 | holidays$christmas.eve == 1 | holidays$cyber.monday == 1 | holidays$black.friday == 1
               | holidays$thanksgiving == 1 | holidays$new.years == 1 | holidays$newyearseve == 1] <- 1
  
  fogod <- merge(fogod[, c("sls_d", "sls_unit_q", "qty", "discount", "mdse_item_i", "co_loc_i", "sales_storecount", promoVars), with = FALSE], holidays[, c("date", "holidayFlag"), with = FALSE], by.x='sls_d' , by.y='date', all.x=TRUE)

  # Create a flag indicating whether or not the record should be removed.  We will remove all promoted, discounted and holiday records
  fogod$remove <- 0
  
  # Remove discounted records and holiday weeks
  fogod$remove[fogod$discount > 0.02 | fogod$holidayFlag == 1] <- 1

  # Remove promoted records
  # This is taking a long.  Try a different method
  promos <- fogod[, c(promoVars), with = FALSE]
  totalPromoForWeek <- apply(fogod[,c(promoVars), with = FALSE], 1, sum)
  fogod$remove[totalPromoForWeek > 0] <- 1

  # Identify outliers after removing flagged records.  Using this algorithm looked better than
  # using the identifyOutlier algorithm
  statsGoodRecords <- fogod[remove==0, .(sdQty = sd(sls_unit_q, na.rm = TRUE),
        meanQty = mean(sls_unit_q, na.rm = TRUE),
        cutoff = mean(sls_unit_q, na.rm = TRUE) + 2*sd(sls_unit_q, na.rm = TRUE)),
        by = .(mdse_item_i, co_loc_i)]
  fogod2 <- fogod[, c("mdse_item_i", "co_loc_i", "sls_d", "qty", "remove", promoVars, "discount", "sales_storecount"), with = FALSE]
  combine <- merge(fogod2, statsGoodRecords, by = c("mdse_item_i", "co_loc_i"), all.x = TRUE)
  combine$origQty <- combine$qty
  combine$init_baseunits <- with(combine, ifelse(remove == 0 & qty > cutoff, cutoff, qty))
  combine$init_baseunits <- with(combine, ifelse(remove == 1, NA, init_baseunits))

  return(combine)

  if(FALSE){
  #fogod <- ddply(fogod, .(fogod$mdse_item_i, fogod$co_loc_i), function(x){identifyOutliers(x, varsOfInterest = c("qty"), varsToConsider = c("remove"))})
  fogod$init_baseunits <- with(fogod, ifelse(remove == 0 & outlier == 0, qty, NA))
  
  fogod <- data.table(fogod)
  # Keep certain variables for debugging purposes  
  fogod2 <- fogod[, c("mdse_item_i", "co_loc_i", "sls_d", "qty", "remove", promoVars, "discount", "init_baseunits"), with = FALSE]
  }
}


imputeBaseunits <- function(input){
  
  # Create baseprice algorithm by trying to extend price data to beginning and end, as appropriate
  # This is a very rough approximation of baseprices
  # Logic below is to find the first and last indices of non-missing prices and use the values
  # at those indices to extend the data series to the beginning and end of the time period
  
  baseunitsForward <- na.locf(input, na.rm = FALSE)
  baseunitsBackwards <- na.locf(input, na.rm = FALSE, fromLast = TRUE)
  concatbaseunits <- cbind.data.frame(baseunitsForward, baseunitsBackwards)
  finalbaseunits <- with(concatbaseunits, ifelse(is.na(baseunitsForward), baseunitsBackwards, baseunitsForward))
}
