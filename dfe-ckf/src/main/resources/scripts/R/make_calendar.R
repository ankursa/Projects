#author: Chad Morgan

# ---------------------------------------------------------------------------------------------------------

# ---- general holiday date function ---- #

# ---------------------------------------------------------------------------------------------------------

holiday_date <- function(year,holiday){
  
  # Take out mardigras,ashwednesday because it competes with valentines
  holidays.list = c('newyearsday','mlkday','superbowl','valentines','presidentsday',
                    'stpatricks','cincodemayo','mothersday',
                    'goodfriday','holysaturday','easter','memorialday','fathersday',
                    'julyfourth','laborday','columbusday','halloween','veteransday',
                    'thanksgiving','black.friday','cyber.monday','christmas.eve', 'week_before_christmas',
                    'christmas','newyearseve')
  
  if( !(holiday %in% holidays.list)){stop("please enter a holiday from the supported list")}
  if(length(holiday)>1){stop("please enter a single holiday")}
  
  # --- helper functions and data --- #
  leap_year <- function(year) {(year %% 4 == 0) & ((year %% 100 != 0) | (year %% 400 == 0))}
  common_months <- c(31,28,31,30,31,30,31,31,30,31,30,31)
  leap_months <- c(31,29,31,30,31,30,31,31,30,31,30,31)
  
  # easter:  first Sunday after the first full moon after vernal equinox
  #   NOTE: Easter function ripped from timeDate package
  easter <- function(year){
    a = year%%19
    b = year%/%100
    c = year%%100
    d = b%/%4
    e = b%%4
    f = (b + 8)%/%25
    g = (b - f + 1)%/%3
    h = (19 * a + b - d - g + 15)%%30
    i = c%/%4
    k = c%%4
    l = (32 + 2 * e + 2 * i - h - k)%%7
    m = (a + 11 * h + 22 * l)%/%451
    easter.month = (h + l - 7 * m + 114)%/%31
    easter.month = ifelse(easter.month > 9,as.character(easter.month),paste('0',as.character(easter.month),sep=''))
    p = (h + l - 7 * m + 114)%%31
    easter.day = p+1
    easter.day = ifelse(easter.day > 9,as.character(easter.day),paste('0',as.character(easter.day),sep=''))
    easter.date = as.Date(paste(year,easter.month,easter.day,sep='-'))
    return(easter.date)
  }
  
  # Meta-function (i.e. closure) for finding dates of "first Sunday in October" type of holidays
  nthofntober <- function(month_x,dayofweek_x,dayofwkofmonth_x){
    holiday_fn <- function(year){
      month <- NULL
      month_int_char = ifelse(month_x<10,paste('0',month_x,sep=''),month_x)
      days_of_month = ifelse(leap_year(year)==1,leap_months[month_x],common_months[month_x])
      for (i in 1:days_of_month){
        if(i<10){
          day = as.Date(paste(year,'-',month_int_char,'-0',i,sep=''))
        }else{
          day = as.Date(paste(year,'-',month_int_char,'-',i,sep=''))
        }
        dayofwk = weekdays(day)
        dayofmonth <- as.integer(substr(as.character(day),9,10))
        dayofwkofmonth = ceiling(dayofmonth/7)
        month = rbind(month,data.frame(date=day,dayofwk=dayofwk,dayofwkofmonth=dayofwkofmonth))
      }
      if(dayofwkofmonth_x=="last"){
        max_weekday = max(subset(month,dayofwk==dayofweek_x)$dayofwkofmonth)
        holiday = month[month$dayofwk==dayofweek_x & month$dayofwkofmonth==max_weekday,'date']
      }else{
        holiday = month[month$dayofwk==dayofweek_x & month$dayofwkofmonth==dayofwkofmonth_x,'date']
      }
      return(holiday)
    }
    return(holiday_fn)
  }
  
  # Martin Luther King Jr Day: third Monday of January
  mlkday <- nthofntober(1,"Monday",3)
  # superbowl: first Sunday in February (since 2001)
  superbowl <- nthofntober(2,"Sunday",1)
  # Presidents' day: third Monday of February 
  presidentsday <- nthofntober(2,"Monday",3)
  # mothers day: Second Sunday in May
  mothersday <- nthofntober(5,"Sunday",2)
  # Memorial day: last Monday of May
  memorialday <- nthofntober(5,"Monday","last")
  # fathers day: Third Sunday in June
  fathersday <- nthofntober(6,"Sunday",3)
  # Labor day: first Monday in September
  laborday <- nthofntober(9,"Monday",1)
  # Columbus day: second Monday in October
  columbusday <- nthofntober(10,"Monday",2)
  # thanksgiving: fourth Thursday in November
  thanksgiving <- nthofntober(11,"Thursday",4)
  
  # ------- return the date of the appropriate holiday -------- #
  switch(holiday,
         newyearsday = {as.Date(paste(year,'-01-01',sep=''))}
         ,mlkday = {mlkday(year)}
         ,superbowl = {superbowl(year)}
         ,valentines = {as.Date(paste(year,'-02-14',sep=''))}
         ,presidentsday = {presidentsday(year)}
         #,mardigras = {easter(year)-47}
         #,ashwednesday = {easter(year)-46}
         ,stpatricks = {as.Date(paste(year,'-03-17',sep=''))}
         ,cincodemayo = {as.Date(paste(year,'-05-05',sep=''))}
         ,mothersday = {mothersday(year)}
         ,goodfriday = {easter(year)-2}
         ,holysaturday = {easter(year)-1}
         ,easter = {easter(year)}
         ,memorialday = {memorialday(year)}
         ,fathersday = {fathersday(year)}
         ,julyfourth = {as.Date(paste(year,'-07-04',sep=''))}
         ,laborday = {laborday(year)}
         ,columbusday = {columbusday(year)}
         ,halloween = {as.Date(paste(year,'-10-31',sep=''))}
         ,veteransday = {as.Date(paste(year,'-11-11',sep=''))}
         ,thanksgiving = {thanksgiving(year)}
         ,black.friday = {thanksgiving(year)+1}
         ,cyber.monday = {thanksgiving(year)+4}
         ,christmas.eve = {as.Date(paste(year,'-12-24',sep=''))}
         ,christmas = {as.Date(paste(year,'-12-25',sep=''))}
         ,week_before_christmas = {as.Date(paste(year,'-12-18',sep=''))}
         ,newyearseve = {as.Date(paste(year,'-12-31',sep=''))}
  )
}


# ---------------------------------------------------------------------------------------------------------

# ---- function to create a full calendar with date attributes from a set of dates ---- #

# ---------------------------------------------------------------------------------------------------------

make_calendar <- function(char.dates,holidays=TRUE){
  
  # --- helper functions and data --- #
  leap_year <- function(year) {(year %% 4 == 0) & ((year %% 100 != 0) | (year %% 400 == 0))}
  common_months <- c(31,28,31,30,31,30,31,31,30,31,30,31)
  leap_months <- c(31,29,31,30,31,30,31,31,30,31,30,31)
  sum_to <- function(max_pos,x){sum(x[1:max_pos])}
  
  # ---------------------------------------------------------------------------------------------------------
  # ---- main body of function ---- #
  # ---------------------------------------------------------------------------------------------------------
  
  # define start and end of calendar
  dates <- as.Date(char.dates)
  start.date <- min(dates)
  end.date <- max(dates)
  
  # create a full calendar of days between min and max dates
  calendar <- data.frame(date=as.Date(start.date),char.date=as.character(start.date))
  colnames(calendar) <- c('date','char.date')
  calendar$char.date <- as.character(calendar$char.date)
  while(calendar[nrow(calendar),'date']<end.date){
    new.row = nrow(calendar)+1
    calendar[new.row,'date'] = calendar[new.row-1,'date']+1
    calendar[new.row,'char.date'] = as.character(calendar[new.row,'date'])
  }
  
  # make basic date attributes
  calendar$year <- as.integer(substr(calendar$char.date,1,4))
  calendar$monthofyr <- as.integer(substr(calendar$char.date,6,7))
  calendar$dayofmonth <- as.integer(substr(calendar$char.date,9,10))
  
  # order by date and number the days
  calendar <- calendar[order(calendar$year,calendar$monthofyr,calendar$dayofmonth),]
  calendar$daynr <- seq(1:nrow(calendar))
  
  # further date attributes
  calendar$dayofwk <- as.integer(factor(weekdays(calendar$date),levels=c('Sunday','Monday','Tuesday','Wednesday','Thursday','Friday','Saturday')))
  calendar$leap_yr_ind <- as.integer(leap_year(calendar$year))
  calendar[calendar$leap_yr_ind==0,'daysatmonthend'] <- as.numeric(lapply(calendar[calendar$leap_yr_ind==0,'monthofyr'],FUN=sum_to,common_months))
  calendar[calendar$leap_yr_ind==1,'daysatmonthend'] <- as.numeric(lapply(calendar[calendar$leap_yr_ind==1,'monthofyr'],FUN=sum_to,leap_months))
  calendar$daysatmonthend <- as.integer(calendar$daysatmonthend)
  calendar$dayofyr <- ifelse(calendar$leap_yr_ind==0
                             ,calendar$daysatmonthend-(common_months[calendar$monthofyr]-calendar$dayofmonth)
                             ,calendar$daysatmonthend-(leap_months[calendar$monthofyr]-calendar$dayofmonth))
  calendar <- calendar[,-9]
  calendar$yearnr <- calendar$year - min(calendar$year) + 1
  calendar$weekofyr <- ceiling(calendar$dayofyr/7)
  calendar$weekofyr <- ifelse(calendar$weekofyr==53,52,calendar$weekofyr)
  calendar$weeknr <- calendar$weekofyr+(calendar$year-calendar[1,'year'])*52 - calendar[1,'weekofyr']
  calendar$cal_weeknr <- ceiling((calendar$daynr-8+calendar[calendar$daynr==1,'dayofwk'])/7)
  calendar$incomplete_cal_weeknr <- ifelse(calendar$cal_weeknr==0 | 
                                             (calendar[calendar$daynr==nrow(calendar),'dayofwk']!=7 & calendar$cal_weeknr==max(calendar$cal_weeknr)),1,0)
  calendar$dayofwkofmonth <- ceiling(calendar$dayofmonth/7)
  
  # ---------------------------------------------------------------------------------------------------------
  # ---- holiday identification ---- #
  # ---------------------------------------------------------------------------------------------------------
  
  if(holidays==TRUE){
    
    holidays.list = c('newyearsday','mlkday','superbowl','valentines','presidentsday',
                      'stpatricks','cincodemayo','mothersday',
                      'goodfriday','holysaturday','easter','memorialday','fathersday',
                      'julyfourth','laborday','columbusday','halloween','veteransday',
                      'thanksgiving','black.friday','cyber.monday','christmas.eve',
                      'christmas','newyearseve')
    
    for(h in holidays.list){
      date.var = paste(h,".date",sep='')
      hdate = data.frame(year=unique(calendar$year)
                         ,d=unlist(lapply(unique(calendar$year)
                                          ,FUN=holiday_date
                                          ,holiday=h)))
      colnames(hdate)[2] = date.var
      calendar <- merge(calendar,hdate,by='year')
      calendar[,h] <- ifelse(calendar$date==calendar[,date.var],1,0)
      calendar = calendar[,-which(names(calendar) == date.var)]
    }
    calendar$holiday = NA
    for(h in holidays.list){calendar$holiday = ifelse(calendar[,h]==1,h,calendar$holiday)}
    
    thanksgiving.date = data.frame(year=unique(calendar$year)
                                   ,thanksgiving.date=unlist(lapply(unique(calendar$year)
                                                                    ,FUN=holiday_date
                                                                    ,holiday="thanksgiving")))
    calendar <- merge(calendar,thanksgiving.date,by='year')
    
    calendar$days.since.thanksgiving <- as.integer(calendar$date-calendar$thanksgiving.date)
    calendar$days.until.christmas <- as.integer(as.Date(paste(calendar$year,"-12-25",sep=''))-calendar$date)
    calendar$christmas.season <- ifelse(calendar$days.since.thanksgiving>=0 & calendar$days.until.christmas>0,1,0)
    calendar$thx_to_xmas_gap = as.integer(as.Date(paste(calendar$year,"-12-24",sep='')) - calendar$thanksgiving.date)
    calendar$thx_to_xmas_pct =  1 - calendar$days.until.christmas / calendar$thx_to_xmas_gap
    calendar$thx_to_xmas_pct = ifelse(calendar$christmas.season==1,calendar$thx_to_xmas_pct,NA)
    
  }
  return(calendar)
}




# ---------------------------------------------------------------------------------------------------------

# ----- function for adding "days before/after holiday" columns to the calendar table ----- #

# ---------------------------------------------------------------------------------------------------------

# usage note: fn will return entire calendar table back plus added columns

days_around_holiday <- function(calendar,holiday,days_before=0,days_after=0){
  
  holidays.list = c('newyearsday','mlkday','superbowl','valentines','presidentsday',
                    'stpatricks','cincodemayo','mothersday',
                    'goodfriday','holysaturday','easter','memorialday','fathersday',
                    'julyfourth','laborday','columbusday','halloween','veteransday',
                    'thanksgiving','black.friday','cyber.monday','christmas.eve',
                    'christmas','newyearseve')
  
  if( !(holiday %in% holidays.list)){stop("please enter a holiday from the supported list")}
  if(length(holiday)>1){stop("please enter a single holiday")}
  
  # check if holiday.date exists. If not merge onto calendar
  holiday.date.name = paste(holiday,".date",sep='')
  check.date = length(setdiff(holiday.date.name,colnames(calendar)))
  if(check.date==1){
    hol_date = data.frame(year=unique(calendar$year)
                          ,d=unlist(lapply(unique(calendar$year)
                                           ,FUN=holiday_date
                                           ,holiday=holiday)))
    colnames(hol_date)[2] = holiday.date.name
    calendar = merge(calendar,hol_date,by='year',all.x=TRUE)
  }
  calendar = calendar[order(calendar$daynr),]
  
  # make columns for days preceding holiday
  if(days_before>0){
    precede.list = seq(1,days_before)
    for(k in precede.list){
      precede.colname = paste(holiday,".minus",k,sep='')
      precede.var = ifelse( as.integer(as.numeric(calendar[,holiday.date.name]) - as.numeric(calendar$date)) == k ,1,0)
      # make sure this preceding date isn't covered by an existing holiday
      holiday.sd = apply(calendar[,holidays.list],2,sd)
      precede.cor = cor(y=precede.var,x=calendar[,holidays.list][,which(holiday.sd>0)])
      if(suppressWarnings(max(precede.cor)<.9999)){
        calendar[,precede.colname] = precede.var
      }else{next}
    }
  }
  
  # make columns for days succeeding holiday
  if(days_after>0){
    succeed.list = seq(1,days_after)
    for(k in succeed.list){
      succeed.colname = paste(holiday,".plus",k,sep='')
      succeed.var = ifelse( as.integer(as.numeric(calendar$date) - as.numeric(calendar[,holiday.date.name])) == k ,1,0)
      # make sure this succeeding date isn't covered by an existing holiday
      holiday.sd = apply(calendar[,holidays.list],2,sd)
      succeed.cor = cor(y=succeed.var,x=calendar[,holidays.list][,which(holiday.sd>0)])
      if(suppressWarnings(max(succeed.cor)<.9999)){
        calendar[,succeed.colname] = succeed.var
      }else{next}
    }
  } 
  if(check.date==1){calendar = calendar[,-which(names(calendar) == holiday.date.name)]}
  return(calendar)
}
