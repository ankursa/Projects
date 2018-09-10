require(zoo)
require(data.table)
source("cleanBaseprices.R")

# Main entry point into this class.  This function creates and imputes all necessary data at the daily level
getFullGridOfData <- function(inputDS, datesDS){
  
  grid <- createFullGridOfData(inputDS, datesDS)
  
  # Choose baseprice based on certain field for each mdse_item_i
  show("Choose baseprice")
  firstBP <- ddply(grid, .(mdse_item_i), function(x){chooseBaseprice(x, "firstBP")})
  firstBP <- data.table(firstBP)
  show("Impute baseprice")
  secondBP <- firstBP[, secondBP := imputePrices(firstBP), by = c("mdse_item_i", "co_loc_i")]
  
  # It is possible that the baseprices can change for a short-time period.  Assume they are outliers
  # and change them to the previous period's prices
  # Don't do this step for the aggregate model.  If it is aggregate, just assume that the price is correct
  show("Override short window baseprice")
  fullGridOfData <- secondBP[, baseprice := cleanBaseprices(secondBP, basewindow = 6), by=c("mdse_item_i", "co_loc_i")]
  fullGridOfData$firstBP <- NULL
  fullGridOfData$secondBP <- NULL
  # We have different possibilities for price.  Use customer paid price because sls_retl_a tends to be pretty bad  
  # Use median because it is more robust than the weighted customer price
  fullGridOfData$price <- fullGridOfData$med_customer_paid_price
  fullGridOfData$price <- with(fullGridOfData, ifelse(is.na(price) | price <= 0.10, baseprice, price))

  # Impute units
  show("Impute units")
  fullGridOfData <- fullGridOfData[, qty := imputeUnits(sls_unit_q), by=c("mdse_item_i", "co_loc_i")]

  # Create discount but change values if the discount is negative or if it is too deep.  Also change the prices
  # to reflect that we needed to change the discount 
  # show("Change extreme prices")
  fullGridOfData$discount <- (fullGridOfData$baseprice - fullGridOfData$price)/fullGridOfData$baseprice
  fullGridOfData$price <- with(fullGridOfData, ifelse(discount < 0, baseprice, price))
  fullGridOfData$price <- with(fullGridOfData, ifelse(discount > 0.75, baseprice*0.25, price))
  fullGridOfData$discount <- with(fullGridOfData, ifelse(discount < 0, 0, discount))
  fullGridOfData$discount <- with(fullGridOfData, ifelse(discount > 0.75, 0.75, discount))
  fullGridOfData$price <- signif(fullGridOfData$price, digits = 5)
  fullGridOfData$baseprice <- signif(fullGridOfData$baseprice, digits = 5)

  fullGridOfData <- data.table(fullGridOfData)
  return(fullGridOfData)
}

chooseBaseprice <- function(pricesDS, basepriceVar = "firstBP"){
  
    match1 <- sum(!is.na(pricesDS$reg_retl_a) & !is.na(pricesDS$own_retl_a) & pricesDS$reg_retl_a == pricesDS$own_retl_a) # give precedence to own_retl_a
    match2 <- sum(!is.na(pricesDS$retl_a) & !is.na(pricesDS$own_retl_a) & pricesDS$retl_a == pricesDS$own_retl_a) # give precedence to own_retl_a
    match3 <- sum(!is.na(pricesDS$retl_a) & !is.na(pricesDS$reg_retl_a) & pricesDS$retl_a == pricesDS$reg_retl_a) # give precedence to reg_retl_a
    
    values <- cbind.data.frame(match1, match2, match3)
    names(values) <- c("own_retl_a", "own_retl_a", "reg_retl_a")
    best <- colnames(values[apply(values,1,which.max)])
    index <- which(names(pricesDS) == best)
    pricesDS <- data.frame(pricesDS)
    pricesDS$baseprice <- pricesDS[, c(index)]
    # Rename to chosen name
    names(pricesDS)[length(names(pricesDS))] <- c(basepriceVar)
    return(pricesDS)
}

# For every store-product, it pads with every date value in datesDS
createFullGridOfData <- function(inputDS, datesDS){
  
  # Get full grid of data.  In other words, every sku-store has every date value in dates
  # This will make the data very long
  storeProducts <- unique(inputDS[,c("co_loc_i", "mdse_item_i"), with = FALSE])
  storeProducts$flag.x <- 1
  datesDS$flag.x <- 1
  datesDS <- data.table(datesDS)
  fullGrid <- merge(storeProducts, datesDS, by = "flag.x", allow.cartesian = TRUE)
  fullGrid$flag.x <- NULL
  stopifnot(nrow(storeProducts)*nrow(datesDS) == nrow(fullGrid))
  
  inputDS$hasData <- 1
  # Now that we have the full grid, merge the data back on
  fullGrid <- merge(fullGrid, inputDS, by = c("co_loc_i", "mdse_item_i", "sls_d"), all.x = TRUE)
  # hasData = 1 means it is imputed data, 0 means it is unchanged from db.  Use missing.sales instead to leverage existing code
  fullGrid$missing.sales <- ifelse(is.na(fullGrid$hasData), 1, 0)
  fullGrid$hasData <- NULL
  return(fullGrid)
}

# For every value of mdse_item_i-date, it makes sure that a chain level base price exists.
createFullGridOfBaseprices <- function(inputDS, datesDS){
  
  baseprices <- data.table(getChainBaseprices(inputDS))  
  products <- unique(inputDS[,c("mdse_item_i"), with = FALSE])
  products$flag.x <- 1
  datesDS$flag.x <- 1
  datesDS <- data.table(datesDS)
  fullGrid <- merge(products, datesDS, by = "flag.x", allow.cartesian = TRUE)
  fullGrid$flag.x <- 1
  
  # Now that we have the full grid, merge the data back on
  fullGridOfBaseprices <- merge(fullGrid, baseprices, by = c("mdse_item_i", "sls_d"), all.x = TRUE)
  fullGridOfBaseprices <- fullGridOfBaseprices[, chain_baseprice := imputePrices(chain_baseprice), by=c("mdse_item_i")]
  
}

# Fills in missing units
imputeUnits <- function(x){
  
  # For simplicity sake, just take the mean of the available units, trimming
  # off 5% at each end. 
  # If it is less than some arbitrary amount (in this case 5), set the 
  # missing units to 0.  
  # Better way to do this: Take a local region or same time last year, find the
  # mean and std deviation and use truncated normal to find probability that it
  # really should be zero.  For high-volume products, probability of non-zero
  # should be low, so there needs to be an algorithm to figure out what the value
  # should have been. For now, we won't substitute high-volume products and just
  # leave it as missing
  trimmedValue <- mean(x, na.rm = TRUE, trim = 0.05)
  newQty <- as.numeric(ifelse(trimmedValue < 5 & is.na(x), 0, x))
  
  return(newQty)
  
}

imputePrices <- function(input){
  
  # Create baseprice algorithm by trying to extend price data to beginning and end, as appropriate
  # This is a very rough approximation of baseprices
  # Logic below is to find the first and last indices of non-missing prices and use the values
  # at those indices to extend the data series to the beginning and end of the time period

  priceForward <- na.locf(input, na.rm = FALSE)
  priceBackwards <- na.locf(input, na.rm = FALSE, fromLast = TRUE)
  concatPrices <- cbind.data.frame(priceForward, priceBackwards)
  finalPrice <- with(concatPrices, ifelse(is.na(priceForward), priceBackwards, priceForward))
}

# We have seen weird reg_retl_a prices (sometimes can be 0).  This algorithm cleans them by using the chain instead
# This algorithm only makes sense for store level data or transaction data
getChainBaseprices <- function(dailyTrans){
  # Select out the good prices and take the median values for each date using the entire chain
  good_baseprices_trans <- subset(dailyTrans, reg_retl_a >= sls_retl_a & reg_retl_a > 0)
  good_baseprices_trans <- data.table(good_baseprices_trans)
  
  good_baseprices <- good_baseprices_trans[, .(chain_baseprice = median(reg_retl_a, na.rm = TRUE)),
                                           by = .(mdse_item_i, sls_d)]
  
  # It is possible that there are no good baseprices because of bad data.  If that is the case, do something
  # so that we always get a value
  goodItems <- unique(good_baseprices$mdse_item_i)
  allItems <- unique(dailyTrans$mdse_item_i)
  badItems <- setdiff(allItems, goodItems)
  if(length(badItems) == 0){
    return(good_baseprices)
  }else{
      badTrans <- subset(dailyTrans, mdse_item_i %in% c(badItems))
      # Just use whatever price we have because reg_retl_a is bad
      badTrans$chain_baseprice <- badTrans$own_retl_a
      badTrans <- subset(badTrans, select = c("mdse_item_i", "sls_d", "chain_baseprice"))
      allBaseprices <- rbind(good_baseprices, badTrans)
      return(allBaseprices)
  }
}
