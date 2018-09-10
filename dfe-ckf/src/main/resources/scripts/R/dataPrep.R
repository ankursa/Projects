# Sources all the files needed for data preparation

source("dataPrepUtil.R")
source("fogodAlgorithms.R")
source("createDailyBaseunits.R")
source("cleanSeries.R")
source("createSeasonality.R")
source("flagHolidayWeeks.R")
source("util.R")
source("shareModel.R")

# Creates the weekly share and baseshare metrics for each group-store-week
createShareMetrics <- function(input){

  stats <- input[, .(totalBaseunits = sum(baseunits, na.rm = TRUE), totalQty = sum(qty, na.rm = TRUE)), by = c("subgroup", "co_loc_i", "week")]
  input2 <- merge(input, stats, by = c("subgroup", "co_loc_i", "week"))
  input2$baseshare = input2$baseunits/input2$totalBaseunits
  input2$share = input2$qty/input2$totalQty
  input2$totalBaseunits <- NULL
  input2$totalQty <- NULL

  return(input2)
  
}

# Throw out any store-item that does not have a scan in the modeling period.
# Perhaps I should have a minimum scan and/or minimum volume?  Geez, I am not
# a miracle worker, after all.
throwOutInsufficientData <- function(input){
  input <- data.table(input)
  counts <- input[, .(totalQty = sum(qty, na.rm = TRUE)), by = c("mdse_item_i", "co_loc_i")]
  setkeyv(counts, c("mdse_item_i", "co_loc_i"))
  setkeyv(input, c("mdse_item_i", "co_loc_i"))
  input2 <- input[counts]
  input2 <- input2[, totalQty := NULL]
  
  # Also make sure that there is sufficient number of scans at the item level.  
  # Our lift model requires at least one observation for each variable.
  # We have intercept, price, and at least 4 promo, so at the minimum,
  # we need 6 scans at different weeks.  We will require at least 8 scans
  totalQtyByWeek <- input[qty > 0, .(totalQty = sum(qty)), by = c("mdse_item_i", "week")]
  totalNumScans <- totalQtyByWeek[, .(numScans = .N), by = c("mdse_item_i")]
  input3 <- merge(input2, totalNumScans, by = c("mdse_item_i"), all.x = TRUE)  
  enough <- subset(input3, numScans >= 8)
  enough <- enough[, numScans := NULL]
  
  notEnough <- subset(input3, numScans < 8 | is.na(numScans))
  notEnough <- notEnough[, numScans := NULL]
  
  return(list(enough = enough, notEnough = notEnough))
  
}



