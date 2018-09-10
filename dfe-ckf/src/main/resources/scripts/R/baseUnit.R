
createCoeffs <- function(window){
  
  coeffs_list <- list()
  
  i <- 1
  while(i <= window){
    j <- i
    coeffs <- c()
    while(j > 0){
      num <- 0
      for(k in j:i){
        num <- num + 1/k
      }
      coeffs <- append(coeffs, num/i/2)
      j <- j - 1
    }
    coeffs_list[[i]] <- coeffs
    i <- i + 1
  }
  
  return(coeffs_list)
  
}

createBaseunits <- function(sls_qt, window = 8){
  
  #debugMsg <- paste("mdse_item_i = ", mdse_item_i, " co_loc_i = ", co_loc_i)
  #show(debugMsg)
  baseunits <- 0
  sls_qt <- ifelse(is.na(sls_qt), 0, sls_qt)

  # coeffs_list <- createCoeffs(window)
  
  # coeffs <- append(append(coeffs_list[window][[1]], 0), rev(coeffs_list[window][[1]]))
  coeffs <- c()
  for(i in 1:window){
    coeffs <- append(coeffs, 1.0/(window+1))
  }
  coeffs <- append(append(coeffs, 2.0/(window+1)), coeffs)
  coeffs <- coeffs/2
 
  baseunits <- as.numeric(stats::filter(sls_qt, filter = coeffs, sides = 2))

  # filter will generate NA's for the beginning and end of the series. do calculatin for those parts
  for(i in 1:window){
    lower <- ifelse(i == 1, 2, i)
    upper <- window + i
    
    before <- mean(sls_qt[1:lower])
    after <- mean(sls_qt[i:upper])
    
    baseunits[i] <- (before + after)/2.0
  }
  
  for(i in (length(sls_qt)-window):length(sls_qt)){
    baseunits[i] <- sls_qt[i]
  }


  return(baseunits)
}
