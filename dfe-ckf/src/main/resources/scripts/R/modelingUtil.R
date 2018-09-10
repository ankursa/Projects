require(dplyr)
require(MCMCpack)

reorderDataByColName <- function(inputData){
  inputData <- data.frame(inputData)
  orderedVars <- order(names(inputData))
  temp <- inputData[c(orderedVars)]
  return (temp)
}


# Get the values for each variable in the model and then put it back together to form coefficients
getCoeffs <- function(model, fixedVars, listOfUniqueKeys){
  
  inputMCMCChain <- model$mcmc
  # Get the list of variable names by extracting out all variables beginning with "beta".
  # This assumes that all modeling variables will be declared as fixed
  
  varlist = union("(Intercept)", fixedVars)
  final <- NULL
  for (i in seq_along(varlist)){
    fixedVar <- paste("beta.", varlist[i], sep = "")
    randomVar <- paste("b.", varlist[i], sep = "")
    var <- varlist[i]      
    randomValues <- inputMCMCChain[, grepl(randomVar, colnames(inputMCMCChain), fixed = TRUE)]
    # If we did not specify random effect, we should use fixed effect only
    # If there are no random effects, the second column will have a value of zero        
    randomExist <- nrow(data.frame(listOfUniqueKeys)) > 1
    if (randomExist){      
      finalCoeff <- randomValues + inputMCMCChain[, fixedVar]
      meanCoeff <- apply(finalCoeff, 2, mean)
      meanCoeffT <- t(meanCoeff)
      finalMean <- data.frame(t(meanCoeffT))              
    } else {
      # For no random, just take the mean of the fixed draws
      finalMean <- data.frame(mean(inputMCMCChain[, fixedVar]))
    }
    names(finalMean) <- c(varlist[i])
    if (i == 1){
      final <- finalMean
    } else {
      final <- cbind(final, finalMean)
    }
  }
  
  if(randomExist){
    final$key <- gsub(paste(randomVar, ".",sep = ""), "", row.names(finalMean), fixed = TRUE)
  }else{
    final$key <- as.character(listOfUniqueKeys)
  }
  
  coeffs <- final
  # The last two lines cause problems so don't bother changing the names here. Do it later, after we get the coeffs
  #names(coeffs)[1:2] <- c("subgroup", "mdse_item_i")
  #coeffs$subgroup <- as.character(coeffs$subgroup)
  
} 


# There could be two "ALL", one from subgroup and one from "CATEGORY".  Choose the one with the best coefficients
getCategoryCoeffs <- function(goodCoeffs, priceVar, promoItemVars, holidaysVar, useShareModel){
  
  if(useShareModel){
    goodCategory <- subset(goodCoeffs, key == "ALL")
  }else{
    goodCategory <- subset(goodCoeffs, (subgroup == "CATEGORY" & key == -1))
  }
  
  if(nrow(goodCategory) == 0){
    # If there are no good coefficients at the category level, use the medians from the subgroups
    # If there are no good subgroups to use, fill in with default values for the category
    if(useShareModel){
      goodSubgroups <- subset(goodCoeffs, key == subgroup)
    }else{
      goodSubgroups <- subset(goodCoeffs, key == -1)
    }
    if(nrow(goodSubgroups) > 0){
      goodCategory <- sapply(goodSubgroups, function(x){ifelse(is.numeric(x), median(x, na.rm = TRUE), NA)})
      goodCategory <- data.frame(t(goodCategory), stringsAsFactors = FALSE)
      names(goodCategory) <- names(goodSubgroups)
      goodCategory$subgroup <- "CATEGORY"
      if(useShareModel){
        goodCategory$key <- "ALL"
      }else{
        goodCategory$key <- -1
      }
    }else{      
      subgroup <- "CATEGORY"         
      if(useShareModel){
        key <- "ALL"
      }else{
        key <- -1
      }
      priceValue <- data.frame(-1)
      names(priceValue) <- priceVar
      promoValues <- cbind.data.frame(t(rep(1, length(promoItemVars))))
      names(promoValues) <- promoItemVars
      seas <- 1
      trend <- 0.5
      holidayValues <- cbind.data.frame(t(rep(0, length(holidaysVar))))
      goodCategory <- cbind.data.frame(subgroup, key, priceValue, promoValues, seas, trend, holidayValues)
    }
  }else if(nrow(goodCoeffs) >= 1){
    # If there is more than one entry, score the coefficients.  Up to this point, we only choose
    # based on the right sign for price and promo but not for magnitude
    # Choose based the set of coefficients that are within a certain range
    # If the scores are the same, then break the tie using the more elastic price coefficient
    
    priceCol <- which(names(goodCategory) == priceVar)
    promoCols <- which(names(goodCategory) %in% promoItemVars)
    
    priceFlag <- data.frame(ifelse(goodCategory[c(priceCol)] < -1 & goodCategory[c(priceCol)] > -2.2, 1, 0))    
    promoFlags <- data.frame(ifelse(goodCategory[c(promoCols)] > 1 & goodCategory[c(promoCols)] < 2.2, 1, 0))   
    seasFlag <- data.frame(ifelse(abs(goodCategory$seas) < 2, 1, 0))    
    trendFlag <- data.frame(ifelse(abs(goodCategory$trend) < 2, 1, 0))    
    # Combine the flags
    total <- cbind.data.frame(priceFlag, promoFlags, seasFlag, trendFlag)
    total$sum <- apply(total, 1, sum)
    goodCategory$sum <- total$sum
    goodCategory <- goodCategory[order(-goodCategory$sum, goodCategory[c(priceCol)]),]
    goodCategory <- goodCategory[1,  c(names(goodCoeffs))]
  }
  
  return(goodCategory)
}


runModelRegression <- function(input, promoItemVars, randomKey, priceVar, depVar, holidaysVar){
  
  input <- data.table(input)
  if(randomKey == "co_loc_i"){
    currentGroup <- input[1, c(mdse_item_i)]
  }else{
    currentGroup <- input[1, c(subgroup)]
  }
  debugMsg <- paste("Processing current subgroup = ", currentGroup)
  show(debugMsg)
  
  # Assume fixed and random vars are the same
  fixedVars <- c(priceVar, "seas", promoItemVars, "trend", holidaysVar)
  # 0 = intercept, -2 for priceVar, 0 = seas, and 1 for each of the promo vars
  mubeta <- c(0, -2, 0, rep(1, length(promoItemVars)), 0, rep(0, length(holidaysVar)))
  
  modelingData <- input[, c(depVar, fixedVars, randomKey, "subgroup", "week", "date", "co_loc_i"), with = FALSE]
  modelingData <- modelingData[complete.cases(modelingData),]
  
  # Find variables that have no variation and throw them out of the list.  If the standard devation is zero,
  # that means there is no variation
  modelingVars <- fixedVars
  modelingData <- data.frame(modelingData)
  sd <- sapply(modelingData[modelingVars], sd, na.rm = TRUE)
  
  index <- which(sd != 0)
  # Update the variables by dropping the no variation variables from the list
  fixedVars <- intersect(names(index), fixedVars)
  # We also need to throw out the no variation variable(s) from mubeta
  variablesToToss <- intersect(names(which(sd == 0)), modelingVars)
  
  # Should we include trend?  Run a quick linear regression and only include if the values are significant
  if(!useTrend(input, fixedVars)){
    # If we don't, exclude trend from fixedVars and mubeta
    # Note that we assumed that we added trend to the end in code above
    fixedVars <- fixedVars[-length(fixedVars)]
    mubeta <- mubeta[-length(mubeta)]
  }

  # Assume for now that randomVars are the same as fixedVars  
  # Note: We are modeling based on 65 weeks, which means that we may have perfect fitting.  Let's see what happens
  randomVars <- fixedVars
  
  # If it possible that all variables need to be tossed because we have no variation anywhere
  # If so, just build out with NA and let downstream truncation take care of it
  if(length(variablesToToss) == length(modelingVars)){
    debugMsg <- paste0(randomKey, " = ", currentItem, " has no variation at all")
    missingCoeffs <- data.frame(t(rep(NA, 1+ length(promoItemVars)+length(holidaysVar))))
    names(missingCoeffs) <- c("(Intercept)", promoItemVars, holidaysVar)
    missingCoeffs$flag <- 1
    return(coeffs)
  }
  
  # Add one because first element belongs to intercept so everything gets shifted
  variablesIndices <- which(modelingVars %in% variablesToToss) + 1
  if(length(variablesIndices) > 0){
    newMubeta <- mubeta[-variablesIndices]
  }else{
    newMubeta <- mubeta
  }
  
  fixedFormula <- paste(depVar, " ~ 1 + ", paste(fixedVars, collapse = " + "))
  randomFormula <- paste(" ~ ", paste(randomVars, collapse = " + "))
  r = length(fixedVars) + 1
  show(fixedFormula)
  #show(randomFormula)
  #show(newMubeta)
  tryCatch({
    mcmcmodel <- MCMChregress(fixed = fixedFormula,
                              random = randomFormula,
                              group = randomKey, data = modelingData,
                              thin = 1, verbose = 0, seed = NA, beta.start = NA,
                              #                         sigma2.start = 1, mcmc = 3000, r = r, R = diag(r),
                              sigma2.start = 1, mcmc = 1000, burnin = 200, r = r, R = diag(r),
                              mubeta = newMubeta)
  }, error = function(cond){
    # It is possible that we have too many variables.  If so, just use the basic ones (price, trend, seas, promo)
    if(nrow(input) <= length(randomVars)*2+1){
      basicVars <- c(priceVar, promoItemVars, "trend", "seas")
      newRandomVars <- intersect(randomVars, basicVars)      
    }else{
      newRandomVars <- randomVars
    }
    randomFormula <- paste(" 1 +  ", paste(newRandomVars, collapse = " + "))
    newFormula <- paste0("logQty", " ~ ", randomFormula)
    show(newFormula)
    # Using lmer, this might not converge 
    debugMsg <- paste("MCMC model failed for ", randomKey, " = ", currentGroup)
    show(debugMsg)
    backupModel=try(lm(newFormula,
                       data=modelingData),silent=F)
    backupCoeffs <- data.frame(t(coefficients(backupModel)))
    # Get the same schema as MCMC output
    names(backupCoeffs)[1] <- c("(Intercept)")
    keys <- data.frame(unique(modelingData[, c(randomKey)]))
    names(keys) <- c("key")
    keys$flag <- 1
    backupCoeffs$flag <- 1
    backupCoeffs <- merge(backupCoeffs, keys, by = c("flag"))
    backupCoeffs$flag <- NULL
    # Need to get the same schema as the rest?
    # I can't get this just to return backupCoeffs2, so I will save it and then read it back
    saveRDS(backupCoeffs, paste0("backupCoeffsItems_", currentGroup, ".rds"))
    
  }, finally = {
    # Don't do anything.  Definitely don't close the connection because it closes the mapper stream
    #closeAllConnections()
  })
  
  if(length(which(ls() == "mcmcmodel")) > 0){
    modelingData <- data.table(modelingData)
    listOfUniqueKeys <- unique(modelingData[, randomKey, with = FALSE])
    #show(listOfUniqueKeys)
    coeffs <- getCoeffs(mcmcmodel, fixedVars, listOfUniqueKeys) 
  } else{
    backupCoeffs <- readRDS(paste0("backupCoeffsItems_", currentGroup, ".rds"))
  }
}

# Run a quick linear regression to see if we should include trend in the model or not
# We have specified a linear 
useTrend <- function(input, fixedVars, depVar = "logQty"){
  
  trendFormula <- as.formula(paste(depVar, "~ 1 + trend + ", paste(fixedVars, collapse = " + ")))
  trendModel <- lm(trendFormula, data = input)
  trendOutput <- summary(trendModel)$coefficients
  # Given that we always put trend as second variable in the equation, its p_value is determined
  # in the matrix
  # It is possible that we can't run the regression for various reasons (eg. qty is constant).  If that is the case, always
  # use trend
  p_value <- trendOutput[2,4]
  if(!is.na(p_value)){
    return(p_value < 0.05)
  }else{
    return(TRUE)
  }
}



