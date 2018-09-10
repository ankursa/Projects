# Data prep util
# Small functions that are specifically used to clean the input data

readSubgroups <- function(namesList, key){
  
  subgroupsFile <- file.path(paste0("subgroups_", key, ".txt")) 
  system(paste0("hadoop fs -getmerge ", namesList$subgroups, " ", subgroupsFile))
  show(paste0("hadoop fs -getmerge ", namesList$subgroups, " ", subgroupsFile))
  # Special characters can cause failures.  If so, just bail for now
  
  subGroups <- tryCatch({
    subGroups <- fread(subgroupsFile, stringsAsFactors = FALSE, header = FALSE, na.strings = "\\N", sep = "|")
    names(subGroups) <- c("mdse_item_i",  "mdse_sbcl_ref_i",  "mdse_sbcl_n")
    subGroups <- prepSubGroups(subGroups)
  }, error = function(error){
    errorMsg <- paste0("Error in reading subgroups file with error ", error)
    show(errorMsg)
    errorFile <- paste0("error_", key, ".txt")
    write.csv(errorMsg, errorFile, row.names = TRUE, quote = FALSE)
    hadoopErrorFile <- paste0(outputLoc, "/", errorFile)
    system(paste0("hdfs dfs -copyFromLocal -f ", errorFile, " ", hadoopErrorFile))
    quit("no", status = 0, runLast = TRUE)
  })

  return(subGroups)
}

prepSubGroups <- function(subGroups){
  index <- names(subGroups) == "mdse_sbcl_n"
  names(subGroups)[index] <- c("subgroup")
  # Remove trailing and leading spaces
  subGroups$subgroup <- gsub("^[ \t]+|[ \t]+$",subGroups$subgroup,replace='')
  # Remove all besides the alphabets & numbers
  subGroups$subgroup <- gsub("[^A-Za-z0-9]", "_", subGroups$subgroup)
  subGroups <- subset(subGroups, subgroup != "UNKNOWN" & !is.na(mdse_item_i))
  # It is possible that the subgroups are all numeric. This can cause problems downstream elsewhere
  # If subgroups is pure numeric, add a prefix of "X" to it
  subGroups$subgroup <- ifelse(!is.na(as.numeric(subGroups$subgroup)), 
                               paste0("X", subGroups$subgroup), subGroups$subgroup)
  # Check to make sure that the mdse_item_i are unique
  check <- data.frame(table(subGroups$mdse_item_i))
  stopifnot(sum(check$Freq > 1) == 0)
  return(data.frame(subGroups))
}

prepSalesData <- function(salesData){
  salesData$wk_begin_date <- as.Date(salesData$wk_begin_date)
  salesData$wk_end_date <- salesData$wk_begin_date + 6
  return(data.frame(salesData))
}

prepPromoData <- function(details){
  details$thresholdvalue <- as.numeric(details$thresholdvalue)
  details$start_date <- as.Date(details$start_date)
  details$end_date <- as.Date(details$end_date)
  details$modified_date <- as.Date(details$modified_date)
  details <- subset(details, rewardtype != "0")
  return(data.frame(details))
}

cleanUp <- function(inputDS){
  
  if(all(is.na(inputDS$baseunits))){
    inputDS <- inputDS[0,]
  }
  
  return(inputDS)
}

# Utility function to construct the root for pulling data
getOnlineInputNames <- function (hive_db_dir, div_ref_i, key){

  namesList <- list()
  namesList$subgroups <- paste0(hive_db_dir, "online_items/key=", key, "/")
  namesList$sales <- paste0(hive_db_dir, "online_promo_weekly_sales/key=", key, "/")
  namesList$promo <- paste0(hive_db_dir, "online_promo_event_details/key=", key, "/")

  return(namesList)
}

# Utility function to construct the root for pulling data
getInputNames <- function (hive_db_dir, key){
  
  namesList <- list()
  namesList$subgroups <- paste0(hive_db_dir,  "/temp_mem_subgroups/key=", key, "/")
  namesList$sales <- paste0(hive_db_dir, "/temp_mem_train_data_prep/key=", key, "/")
  namesList$promo <- paste0(hive_db_dir, "/temp_agg_promo_list/key=", key, "/")
  namesList$forward <- paste0(hive_db_dir, "/temp_mem_scoring_data_prep/key=", key, "/")
  
  return(namesList)
}


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

