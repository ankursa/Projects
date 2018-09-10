require(lme4)
require(ggplot2)
require(MCMCpack)

shift <- function(df, sv = 1) df[c((length(df)-sv+1):length(df), 1:(length(df)-sv))]

seasonality <- function(input, seas_window=4){
  
  # assume input data has no missing weeks
  #currentValue <- input[1, "subgroup"]
  #debugMsg <- paste0("Processing seasonality for subgroup = ", currentValue)
  #show(debugMsg)
  
  # if seasonality is at store-item level
  if(FALSE){
    num_weeks = unique(input$cal_weeknr)
    x <- rep(1:13, each=seas_window, length.out=length(num_weeks))
    y <- rep(1:52, length.out=length(num_weeks))
    
    input$baseunits <- ifelse(input$baseunits == 0, 0.0000000001, input$baseunits)
    input$logbase <- log(input$baseunits)
    input$weekindx <- y
    input$month1 <- x
    input$month2 <- shift(x,1)
    input$month3 <- shift(x,2)
    input$month4 <- shift(x,3)
    
    # http://stats.stackexchange.com/questions/122009/extracting-slopes-for-cases-from-a-mixed-effects-model-lme4
    input$f_m1 <- factor(input$month1)
    input$f_m2 <- factor(input$month2)
    input$f_m3 <- factor(input$month3)
    input$f_m4 <- factor(input$month4)
    
    fit1 <- lmer(logbase ~ 1 + weekindx + (1|f_m1), input)
    fit2 <- lmer(logbase ~ 1 + weekindx + (1|f_m2), input)
    fit3 <- lmer(logbase ~ 1 + weekindx + (1|f_m3), input)
    fit4 <- lmer(logbase ~ 1 + weekindx + (1|f_m4), input)
    
    a1 <- coef(fit1)$f_m1[1]
    names(a1) <- c("intercept")
    a2 <- coef(fit2)$f_m2[1]
    names(a2) <- c("intercept")
    a3 <- coef(fit3)$f_m3[1]
    names(a3) <- c("intercept")
    a4 <- coef(fit4)$f_m4[1]
    names(a4) <- c("intercept")
    seas <- data.frame(month1=1:13, seas_indx=exp(((a1$intercept - mean(a1$intercept)) + (a2$intercept - mean(a2$intercept)) + (a3$intercept - mean(a3$intercept)) + (a4$intercept - mean(a4$intercept)))/4))
  
    # # 13 intercepts and 1 weekindx coeff
    # # The result of coef(fit1)$f_m1 incoporates the fixed effects into the random effects, i.e., the fixed effect coefficients 
    # # are added to the random effects. The results are individual intercepts and slopes.
    # coef(fit1)$f_m1
    
    # # use summary and coef to obtain the coefficients of the fixed effects. These coeffcients are the mean of all f_m1's
    # coef(summary(fit1))[ , "Estimate"]
    # # To obtain the random effects, you can use ranef. These values are the variance components of the f_m1's. 
    # # Every row corresponds to one f_m1. Inherently the mean of each column is zero since the values correspond 
    # # to the differences in relation to the fixed effects.
    # ranef(fit1)$f_m1
    # colMeans(ranef(fit1)$f_m1)
    
    output <- merge(input, seas, by=c("month1"), all.x=TRUE)
    
    show(names(output))
    output$month2 <- NULL
    output$month3 <- NULL
    output$month4 <- NULL
    output$f_m1 <- NULL
    output$f_m2 <- NULL
    output$f_m3 <- NULL
    output$f_m4 <- NULL
  }
  if(TRUE){
    # Leveraging existing code, so assign input to aggInput
    aggInput <- data.table(input)
    num_weeks = unique(aggInput$week)
    debugMsg <- paste0("Seasonality for ", aggInput[1, subgroup])
    show(debugMsg)
    x <- rep(1:13, each=seas_window, length.out=length(num_weeks))
    y <- rep(1:length(num_weeks))
    
    aggInput$baseunits <- ifelse(aggInput$baseunits == 0 | is.na(aggInput$baseunits), 0.5, aggInput$baseunits)
    aggInput$logbase <- log(aggInput$baseunits)
    aggInput$weekindx <- y
    aggInput$month1 <- x
    aggInput$month2 <- shift(x,1)
    aggInput$month3 <- shift(x,2)
    aggInput$month4 <- shift(x,3)
    
    aggInput$f_m1 <- factor(aggInput$month1)
    aggInput$f_m2 <- factor(aggInput$month2)
    aggInput$f_m3 <- factor(aggInput$month3)
    aggInput$f_m4 <- factor(aggInput$month4)
    
    # For one particular use case, using + 1 rather than -1 ended up with no variation.  
    # If +1 doesn't work, try -1.  If that still doesn't work then use a different
    # estimation routine.  It can be argued that we can use the MCMC from the start
    # but since it was tested using lmer, let's just stick with that for now because
    # we know that method works for this algorithm
    fit1 <- lmer(logbase ~ 1 + weekindx + (1|f_m1), aggInput)
    fit2 <- lmer(logbase ~ 1 + weekindx + (1|f_m2), aggInput)
    fit3 <- lmer(logbase ~ 1 + weekindx + (1|f_m3), aggInput)
    fit4 <- lmer(logbase ~ 1 + weekindx + (1|f_m4), aggInput)

    a1 <- coef(fit1)$f_m1[1]
    names(a1) <- c("intercept")
    if(sd(a1$intercept) == 0){
      fit1 <- lmer(logbase ~ -1 + weekindx + (1|f_m1), aggInput)
      fit2 <- lmer(logbase ~ -1 + weekindx + (1|f_m2), aggInput)
      fit3 <- lmer(logbase ~ -1 + weekindx + (1|f_m3), aggInput)
      fit4 <- lmer(logbase ~ -1 + weekindx + (1|f_m4), aggInput)
      a1 <- coef(fit1)$f_m1[1]
      names(a1) <- c("intercept")
      if(sd(a1$intercept) == 0){
        a1 <- getSeasCoeffs(aggInput, month = "month1")
        a2 <- getSeasCoeffs(aggInput, month = "month2")
        a3 <- getSeasCoeffs(aggInput, month = "month3")
        a4 <- getSeasCoeffs(aggInput, month = "month4")
      }
    }
    a1 <- data.frame(month1=1:13, seas1=exp(a1$intercept - mean(a1$intercept)))
    a2 <- coef(fit2)$f_m2[1]
    names(a2) <- c("intercept")
    a2 <- data.frame(month2=1:13, seas2=exp(a2$intercept - mean(a2$intercept)))
    a3 <- coef(fit3)$f_m3[1]
    names(a3) <- c("intercept")
    a3 <- data.frame(month3=1:13, seas3=exp(a3$intercept - mean(a3$intercept)))
    a4 <- coef(fit4)$f_m4[1]
    names(a4) <- c("intercept")
    a4 <- data.frame(month4=1:13, seas4=exp(a4$intercept - mean(a4$intercept)))
    
    setkeyv(aggInput, "month1")
    aggInput2 <- aggInput[, c("week", "date", "month1", "month2", "month3", "month4", "baseunits", "qty"), with = FALSE]
    output <- merge(aggInput2, a1, by = c("month1"), all.x=TRUE)
    output <- merge(output, a2, by=c("month2"), all.x=TRUE)
    output <- merge(output, a3, by=c("month3"), all.x=TRUE)
    output <- merge(output, a4, by=c("month4"), all.x=TRUE)
    
    output$seas <- (output$seas1 + output$seas2 + output$seas3 + output$seas4)/4
    output <- output[order(week)]
    
    # We can still run into issues with seasonality being constant.  If so, just create random seasonality
    if(sd(output$seas) == 0){
      show("Generate random seasonality")
      set.seed(123)
      output$seas <- runif(nrow(output), min=0.9, max=1.1)
    }
    output$month1 <- NULL
    output$month2 <- NULL
    output$month3 <- NULL
    output$month4 <- NULL
  }

  return(output) 
}

seasModel <- function(input, monthValue, sigma2.start.value){
  
  mcmcmodel <- tryCatch({
    mcmcmodel <- MCMChregress(logbase ~ 1+ weekindx, data = input,
                              random = ~ 1,
                              group = monthValue, r = 1,  R = diag(1), b0 = c(0,0),  mcmc = 1000, burnin = 200, sigma2.start = sigma2.start.value)
  }, error = function(error){
      debugMsg <- paste0("Error in running seas model with sigma2.start = ", sigma2.start.value, " ", error)
      show(debugMsg)
      mcmcmodel <- list()
  })

  return(mcmcmodel)  
}

getSeasCoeffs <- function(input, month){
  
  mcmcmodel <- seasModel(input, month, sigma2.start.value = NA)
  counter <- 0
  while(length(mcmcmodel) == 0 & counter < 25){
    mcmcmodel <- seasModel(input, month, sigma2.start.value = 0 + (counter)*.05)
    counter <- counter + 1
  }
  
  if(length(mcmcmodel) == 0){
    errorMsg <- "We cannot get the seas model to work"
    stop(errorMsg)
  }
  inputMCMCChain <- mcmcmodel$mcmc
  # Get the list of variable names by extracting out all variables beginning with "beta".
  var <- "(Intercept)"
  final <- NULL
  fixedVar <- "beta.(Intercept)"
  randomVar <- "b.(Intercept)"
  randomValues <- inputMCMCChain[, grepl(randomVar, colnames(inputMCMCChain), fixed = TRUE)]
  finalCoeff <- randomValues + inputMCMCChain[, fixedVar]
  meanCoeff <- apply(finalCoeff, 2, mean)
  meanCoeffT <- t(meanCoeff)
  finalMean <- data.frame(t(meanCoeffT))              
  names(finalMean) <- c(var)
  finalMean[, c(month)] <- as.numeric(gsub("b.(Intercept).", "", rownames(finalMean), fixed = TRUE))
  finalMean <- finalMean[order(finalMean[, month]),]
  names(finalMean)[c(1)] <- c("intercept")
  finalMean <- subset(finalMean, select = c("intercept"))
  return(finalMean)
    
} 

