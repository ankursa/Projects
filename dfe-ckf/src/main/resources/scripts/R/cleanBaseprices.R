# Data prep util
# Small functions that are specifically used to clean readdata


# input refers to the vector to be operated on.  In this case, it should represent the baseprice
# column
cleanBaseprices <-function(input, basewindow = 8){
  
  y <- rle(input)
  # If the current length is not BASE_WINDOW in size, use the previous value of baseprice
  baseprice_run <- c()
  for(i in 1:length(y$length)){
    
    if(i == 1){
      carryForwardValue <- input[1]
    }
    currentRun <- y$length[i]
    if(currentRun >= basewindow){
      carryForwardValue <- y$value[i]    
    }
    baseprice_run <- append(baseprice_run, rep(carryForwardValue, y$length[i]))
  }
  
  stopifnot(length(baseprice_run) == length(input))
  return(baseprice_run)
}




cleanUp <- function(inputDS){
  
  if(all(is.na(inputDS$baseunits))){
    inputDS <- inputDS[0,]
  }
  
  return(inputDS)
}

