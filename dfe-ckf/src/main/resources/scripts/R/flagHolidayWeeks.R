# For now, flag holiday weeks.  Just hard-code them from past and future.  This is an exact copy of 0.4_make_calendar.
source("make_calendar.R")

addHolidays <- function(inputDS, weekEndDateVar = "sls_d"){
  
  # Subtract 6 because sls_d represents end_date
  inputDS <- data.frame(inputDS)
  dateData <- subset(inputDS, select = c(weekEndDateVar))
  names(dateData) <- c("date")
  dateData <- sort(dateData$date)
  # -6 because we are dealing with week end dates
  startDate <- dateData[1] - 6
  lastDate <- dateData[length(dateData)]
  newMapping <- data.table(getWeeklyMapping(startDate, lastDate))
  newMapping <- merge(newMapping[, .(week_start_date = min(date), week_end_date = max(date)), by = c("week")], newMapping, by = c("week"))
  calendar <- createHolidayCalendar(startDate, lastDate)
  holidayMapping <- merge(newMapping, calendar, by = c("date"), all.x = TRUE)
  holidayMapping[is.na(holidayMapping$holiday),]$holiday <- "no_holiday"
  holidayMapping <- data.frame(holidayMapping)
  holidaysList <- setdiff(unique(holidayMapping$holiday), "no_holiday") 
  for(level in holidaysList){
    holidayMapping[paste("hol", level, sep = "_")] <- ifelse(holidayMapping$holiday == level, 1, 0)
  }
  
  # Collapse the mapping to one week and get the first record, based on priority.  Note that we have chosen a limited
  # number of non-overlapping holidays.  Lower is higher on the holiday food chain because we will sort by ascending
  # TODO: Take care of multiple holidays in a week
  holidayMapping$priority <- with(holidayMapping, ifelse(holiday == "no_holiday", 1, 0))
  holidayMapping <- data.table(holidayMapping)
  holidayVars <- paste0("hol_", holidaysList)
  if(length(holidaysList) > 0){
    oneHolidayRecord <- holidayMapping[, lapply(.SD, sum, na.rm = TRUE), .SDcols = c(holidayVars), by = c("week", "week_start_date")]
    holidayMapping <- subset(holidayMapping, select = c("priority", "week", "week_start_date", "holiday"))
    holidayMapping2 <- merge(oneHolidayRecord, holidayMapping, by = c("week", "week_start_date"))
    holidayMapping2 <- holidayMapping2[order(holidayMapping2$week, holidayMapping2$priority),]  
    # If there are multiple holidays, choose one and put them together
    #holidayMapping2$totalHoliday <- rowSums(holidayMapping2[,grep("hol_", names(holidayMapping2)),with=FALSE])
    # Take the first record 
    holidayMapping2 <- cbind(holidayMapping2, first=0L)
    holidayMapping2 <- data.table(holidayMapping2)
    setkeyv(holidayMapping2, c("week", "week_start_date"))
    holidayMapping2[holidayMapping2[unique(holidayMapping2),,mult="first", which=TRUE], first:=1L]
    holidayMapping2 <- subset(holidayMapping2, first == 1)
    holidayMapping2$first <- NULL
    holidayMapping2$priority <- NULL
    holidayMapping2$week_start_date <- NULL
  } else{
    holidayMapping2 <- subset(holidayMapping, date == week_end_date, c("week", "holiday"))
    holidaysList <- list()
  }
  
  inputDS2 <- merge(inputDS, holidayMapping2, by = c("week"), all.x = TRUE)
  return(list(holidayDataSet = inputDS2, holidaysList = holidaysList))

}

# Generate a calendar of holidays, given the week start and end date.  Only certain holidays are created. Holidays are mapped to 
# a particular week.  For now, the holidays are chosen so that there are no overlapping holidays
createHolidayCalendar <- function(weekStartDate, weekEndDate){
  
  calendar <- make_calendar(c(weekStartDate, weekEndDate))
  desiredHolidays <- c("newyearsday", "valentines", "easter", "halloween", "thanksgiving", "cyber.monday", "week_before_christmas", "christmas", "laborday", "julyfourth")
  calendar <- subset(calendar, holiday %in% c(desiredHolidays), select = c("date", "holiday"))  

}
