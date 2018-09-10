# -*- coding: utf-8 -*-
import numpy as np
import traceback
import pandas as pd
import datetime as dt
import sys
import json
sys.path.append(".")

#Month the year to Quarter of the year map 
monthQuarterMap = {1:1,2:1,3:1,4:2,5:2,6:2,7:3,8:3,9:3,10:4,11:4,12:4}

#Input data Columns Name
full_list = ["mdse_item_i","mdse_clas_i","mdse_dept_ref_i","week_start_date","week_end_date","sls_retl_a","med_own_retl_a","sls_unit_q","avg_sales","n_stores","circular_flag","circular_flag_count","clearance_flag","clearance_flag_count","dollar_off_flag","dollar_off_flag_count","tpc_flag","tpc_flag_count","pct_off_flag","pct_off_flag_count","max_promo_daynr","max_promo_dollaroff","max_external_pctoff","christmas_flag","easter_flag","thanksgiving_flag","newyearsday_flag","fathersday_flag","mothersday_flag","julyfourth_flag","valentines_flag","memorialday_flag","halloween_flag","superbowl_flag","stpatricks_flag","ashwednesday_flag","blackfriday_flag","columbusday_flag","cybermonday_flag","goodfriday_flag","holysaturday_flag","laborday_flag","mardigras_flag","mlkday_flag","presidentsday_flag","veteransday_flag","all_holidays","wk1_flag","wk2_flag","wk3_flag","wk4_flag","wk5_flag","wk6_flag","wk7_flag","wk8_flag","wk9_flag","wk10_flag","xmas1_flag","xmas2_flag","unique_id","week_end_date_new"]

features_dept_map = {}
features_dept_map['bts only']=["wk1_flag","wk2_flag","wk3_flag","wk4_flag","wk5_flag","wk6_flag","wk7_flag","wk8_flag","wk9_flag","wk10_flag"]

features_dept_map["bts_hols"]=["blackfriday_flag","cybermonday_flag","goodfriday_flag","xmas1_flag","xmas2_flag","easter_flag","mothersday_flag","valentines_flag","julyfourth_flag","laborday_flag","halloween_flag","tpc_flag","circular_flag"]


#Reading a configuration file for dept specific models.
#Default: If no config found for a particular dept use the default settings.
def readConfigFile(dept):
   configFile = pd.read_csv('config.csv')
   config_row = configFile[configFile['dept_id']==int(dept)]
   if len(config_row) == 0:
      obv_var = 'sls_unit_q' 
      fmap_key = 'bts_hols' 
      beta = 0.01 
      p_value = 0.01 
      q_value = 0.001 
      r_value = 0.001 
   else:
      obv_var = config_row['obs_var'].tolist()[0] 
      fmap_key = config_row['feature_set_key'].tolist()[0]
      beta = config_row['beta'].tolist()[0]
      p_value = config_row['p_value'].tolist()[0]
      q_value = config_row['q_value'].tolist()[0]
      r_value = config_row['r_value'].tolist()[0]
   return obv_var, fmap_key, beta, p_value, q_value, r_value 


#Fetching variable list from the input data set which need to be ignored.
def fetch_ignore_list(model_name):
   vars_list = features_dept_map[model_name]
   full_list_copy = full_list[:]
   for var in vars_list:
      full_list_copy.remove(var)
   return full_list_copy


#Parsing an input data frame and extract different components.
#Eg, Item_Id,Dept_Id,Week_end_date,Observation,State,State-covariance,Process & Measurement noise 
def parseDataFrame(dataFrame):

   df_item_id = dataFrame["mdse_item_i"].tolist()[0]
   df_dept_id = dataFrame["mdse_dept_ref_i"].tolist()[0]
   df_date = dataFrame["week_end_date"]
   obv_var,fmap_key,beta,p_value,q_value,r_value = readConfigFile(df_dept_id)
   df_obv =  dataFrame[obv_var].astype("float64")
   ig_list = fetch_ignore_list(fmap_key)
   df_nstores = dataFrame["n_stores"]
   df_inp = dataFrame.drop(ig_list,axis=1,inplace=False)
   for column in df_inp.columns:
      df_inp[column]=df_inp[column].astype("float64")
   datalen = len(df_obv)

   return df_date,df_obv,df_inp,datalen,df_item_id,df_dept_id,fmap_key,obv_var,beta,p_value,q_value,r_value,df_nstores


#creating a prediction output
def predictionOutput(state,covariates,date,start,end,item_id,dept_id,df_nstores,obv_var,max_obv,quarterMap,dataColumns,modelName):
   predictedData = []
   inputVars = features_dept_map.get(modelName)

   holidayEventsIndex = [dataColumns.index(item) for item in inputVars if "tpc" not in item and "circular" not in item]
   promoEventsIndex = [dataColumns.index(item) for item in inputVars if "tpc" in item or "circular" not in item]

   for i in range(start,end):
      pobv = np.dot(covariates[i],state)
      date_str = date.iloc[i]
      week_date = dt.datetime.strptime(date_str,"%Y-%m-%d")
      month = week_date.month
      quarter = monthQuarterMap.get(month)
      quarterSales = [0]
      
      #checking the presence of holidays in a given week.
      holidaysCheck = [covariates[i][index] for index in holidayEventsIndex].count(1)

      #fetchin promo vars values
      promoVars = sum([covariates[i][index] for index in promoEventsIndex])

      if quarter not in quarterMap:
         #Consider last year last quarter
         if quarter == 1:
            quarter = 4
         else:
            #Consider last quarter
            quarter -= 1

      quarterSales = quarterMap.get(quarter)

      mean = np.mean(quarterSales)
      std  = np.std(quarterSales)

      if std == 0:
         std = 0.15*mean

      '''
      # 0 and NULL imputation
      if pobv[0] <= 0 or pobv[0] is None:
        if i == 0:
           pobv[0] = quarterSales[-1]
        else:
           pobv[0] = predictedData[i-1]

      #high value imputation
      #NOTE: When there is no holiday
      elif pobv[0] > mean+4*std and holidaysCheck==0 and mean > 0 and std > 0:
         pobv[0] = mean+4*std

      #NOTE: When there are holidays
      elif pobv[0] > mean+6*std and holidaysCheck > 0 and mean > 0 and std > 0:
         pobv[0] = mean+6*std

      #low value imputation
      #NOTE: Do we really need this??
      #elif pobv[0] < mean-4*:
      #   pobv[0] = mean
      '''
      predictedData.append(pobv[0])

      if pobv[0] is not None:
         if pobv[0] <= 0:
            pobv[0] = 0.1

         if pobv[0] > 2*max_obv:
            pobv[0] = 2*max_obv

         print item_id,dept_id,date.iloc[i],pobv[0]


def createArray(beta):
   np_list = []

   for item in beta:
      np_list.append([item])

   np_array = np.array(np_list)

   return np_array

#Method for calculating week until holiday events i.e. Christmas,New Year
#Return: Lisf of numbers representing week until given holidays
def weekUntilHolidays(data,holidaysVarsList):
   datalen = len(data)
   df_date = data["week_end_date_new"]
   for item in holidaysVarsList:
     df_event = data[item]

     yearEventMap = {}

     itemList = [0 for i in range(datalen)]

     for i in range(datalen):
        eventFlag = df_event.iloc[i]
        if int(eventFlag) == 1:
           eventDate = df_date.iloc[i]
           weekOfEvent = eventDate.isocalendar()[1]
           eventYear = eventDate.year
           yearEventMap[eventYear] = float(weekOfEvent)/52.0

     for i in range(datalen):
        date = df_date.iloc[i]
        weekOfYear = float(date.isocalendar()[1])/52.0
        year = date.year
        if year in yearEventMap:
           eventWeek = yearEventMap.get(year)
           if weekOfYear < eventWeek:
              weekUntil = eventWeek-weekOfYear
              itemList[i] = weekUntil

     data["%s_until"%item] = itemList

#Method of creating quarter map of last 1 year sales
#Map: Key=Quarter of the year, Value=sales value
def getLastQuarterValues(salesData):
   quarterMap = {}
   for i in range(len(salesData)):
      sdata,date_str = salesData[i]
      date = dt.datetime.strptime(date_str,"%Y-%m-%d")
      month = date.month
      quarter = 1
      if month>=4 and month <=6:
         quarter = 2
      elif month>=7 and month <=9:
         quarter = 3
      elif month>=10 and month <=12:
         quarter = 4
     
      slist = []
      if quarter not in quarterMap:
         quarterMap[quarter] = slist
      else:
         slist = quarterMap.get(quarter)

      slist.append(sdata[0])

   return quarterMap


   
#Main Method for scoring.
def run(data,model_params):
   #replacing \N with 0 value
   #Cleanup of input data replacing \N with 0 value
   data.replace("\N", 0, inplace=True)

   #Parse Data frame function call
   df_date,df_obv,df_inp,datalen,item_id,dept_id,modelName,obv_var,beta_value,p_value,q_value,r_value,df_nstores = parseDataFrame(data)

   params = json.loads(model_params)
   try:
      params = params.get(str(item_id))
      params = json.loads(params)
   except:
      return

   #beta values will be replaced by trained values
   beta = params.get("state")
   #max_retl_a = params.get("max_retl_a")
   #future_base_price = params.get("future_base_price")
   max_obv = params.get("max_obv")
   sales = params.get("sales")

   #fetching training quarter-wise sales values
   quarterMap = getLastQuarterValues(sales)

   '''
   #normalize retl_a
   if (max_retl_a!=0):
      df_inp['med_own_retl_a']=future_base_price/max_retl_a
   '''

   beta_np_array = createArray(beta)

   date_format = pd.to_datetime(df_date)

   #seas map comes from stores json string
   seas_map = params.get("seasonal")

   #holidays = ["christmas_flag","newyearsday_flag"]
   #weekUntilHolidays(data,holidays)

   #creating a list of indepenendent variables
   covariates = []
   for i in range(datalen):
  
      date_str = df_date.iloc[i]
      date = dt.datetime.strptime(date_str,"%Y-%m-%d")

      inputs = df_inp.iloc[i].tolist()

      for _i in range(20):
         date_1 = date-dt.timedelta(days=(365+_i))
         date_2 = date-dt.timedelta(days=(365-_i))

         date_s = dt.datetime.strftime(date_1,"%Y-%m-%d")
         date_1_s = dt.datetime.strftime(date_2,"%Y-%m-%d")

         if date_s in seas_map:
            val = seas_map.get(date_s)
            break
         elif date_1_s in seas_map:
            val = seas_map.get(date_1_s)
            break
         else:
            val = 0.0

      inputs.append(val)

      inputs.append(1)

      ################# Experimentation #################
      #adding weekuntil vars
      #for hols in holidays:
      #   holsDframe = data["%s_until"%hols]
      #   inputs.append(holsDframe.iloc[i])
      ################# Experimentation #################
 
      covariates.append(np.array(inputs))

   cov_pred = np.array(covariates)

   try:
      #outputting prediction
      predictionOutput(beta_np_array,cov_pred,df_date,0,datalen,item_id,dept_id,df_nstores,obv_var,max_obv,quarterMap,df_inp.columns.tolist(),modelName)
   except:
      pass
