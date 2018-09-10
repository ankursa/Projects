# -*- coding: utf-8 -*-
import numpy as np
import traceback
import pandas as pd
import statsmodels.api as sm
import datetime as dt
from ckf_np import ckf
from scipy.optimize import fmin_l_bfgs_b
from multiprocessing import Process
from sklearn import linear_model
import sys
import json
import subprocess

sys.path.append(".")

#Input data Columns Name

full_list = ["mdse_item_i","mdse_clas_i","mdse_dept_ref_i","week_start_date","week_end_date","sls_retl_a","retl_a","med_own_retl_a","med_reg_retl_a","med_customer_paid_price","sls_unit_q","avg_sales","n_stores","n_stores_asmt","circular_flag","circular_flag_count","clearance_flag","clearance_flag_count","dollar_off_flag","dollar_off_flag_count","tpc_flag","tpc_flag_count","pct_off_flag","pct_off_flag_count","max_promo_daynr","max_promo_dollaroff","max_external_pctoff","christmas_flag","easter_flag","thanksgiving_flag","newyearsday_flag","fathersday_flag","mothersday_flag","julyfourth_flag","valentines_flag","memorialday_flag","halloween_flag","superbowl_flag","stpatricks_flag","ashwednesday_flag","blackfriday_flag","columbusday_flag","cybermonday_flag","goodfriday_flag","holysaturday_flag","laborday_flag","mardigras_flag","mlkday_flag","presidentsday_flag","veteransday_flag","all_holidays","wk1_flag","wk2_flag","wk3_flag","wk4_flag","wk5_flag","wk6_flag","wk7_flag","wk8_flag","wk9_flag","wk10_flag","xmas1_flag","xmas2_flag","unique_id","week_end_date_new"]

###########
features_dept_map = {}
features_dept_map['bts only']=["wk1_flag","wk2_flag","wk3_flag","wk4_flag","wk5_flag","wk6_flag","wk7_flag","wk8_flag","wk9_flag","wk10_flag"]

features_dept_map["bts_hols"]=["blackfriday_flag","cybermonday_flag","goodfriday_flag","xmas1_flag","xmas2_flag","easter_flag","mothersday_flag","valentines_flag","julyfourth_flag","laborday_flag","halloween_flag","tpc_flag","circular_flag"]

features_dept_map['bts_hols_wthr']=["blackfriday_flag","cybermonday_flag","goodfriday_flag","xmas1_flag","xmas2_flag","wk1_flag","wk2_flag","wk3_flag","wk4_flag","wk5_flag","wk6_flag","wk7_flag","wk8_flag","wk9_flag","wk10_flag","norm_temp","norm_pcpt"]

features_dept_map['hols_only']=["blackfriday_flag","cybermonday_flag","goodfriday_flag","xmas1_flag","xmas2_flag"]

features_dept_map['weather']=["norm_pcpt"]

features_dept_map['bts_hols_nstores']=["blackfriday_flag","cybermonday_flag","goodfriday_flag","xmas1_flag","xmas2_flag","wk1_flag","wk2_flag","wk3_flag","wk4_flag","wk5_flag","wk6_flag","wk7_flag","wk8_flag","wk9_flag","wk10_flag","n_stores"]

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

#Fetching a model
def fetch_model(modelName,beta,p,q,r):
   vars_list = features_dept_map[modelName]
   columns = ['model_Id','name','beta_values','p_values','q_values','r_values']
   model_list = [['test','sales',beta,p,q,r]]
   for item in vars_list:
      model_list.append(['test',item,beta,p,q,r])
   model_list.append(['test','intercept',beta,p,q,r])
   model_list.append(['test','seasonality',beta,p,q,r])
   #model_list.append(['test','christmas_flag_until',0.0,0.01,0.001,0.001])
   #model_list.append(['test','newyearsday_flag_until',0.0,0.01,0.001,0.001])

   arr = np.array(model_list)
   df = pd.DataFrame(arr)
   df.columns=columns
   return df 

#Assigning a model for a given department.
def assign_model(dept_id):
   modelName='bts_hols'
   return modelName

#OLS: Ols (Ridge regression) is used for finding the initial values for kalman filter states.
#Input: 
#df_input: Dataframe of independent Variabales
#df_obv: Dataframe of depdenend variable
#Return:
#List of OLS coeffecients
def ols(df_input,df_obv,fit_intercept=False):
   clf = linear_model.Ridge(alpha=0.5,fit_intercept=fit_intercept)
   clf.fit(df_input,df_obv)
   coff = clf.coef_

   if fit_intercept:
      intercept = clf.intercept_
   else:
      intercept = None

   return coff,intercept
  
#Parsing an input data frame and extract different components.
#Eg, Item_Id,Dept_Id,Week_end_date,Observation,State,State-covariance,Process & Measurement noise 
def parseDataFrame(dataFrame):

   df_unique_id = dataFrame["unique_id"].tolist()[0]
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

   return df_date,df_obv,df_inp,datalen,df_item_id,df_dept_id,fmap_key,obv_var,beta,p_value,q_value,r_value,df_nstores,df_unique_id

#Parsing a model frame and extract different components.
#State values,State covariance matrix,Process & Measurement Noise.
def parseModelFrame(modelFrame):
   _beta    = modelFrame["beta_values"].astype("float64").tolist()[1:]
   _pvalues = modelFrame["p_values"].astype("float64").tolist()[1:]
   _qvalues = modelFrame["q_values"].astype("float64").tolist()[1:]
   _rvalues = modelFrame["r_values"].astype("float64").tolist()[1:]

   state_dim = len(_beta)
 
   #beta = np.array([[item] for item in _beta],dtype='float64')
   beta = _beta
   pvalues = np.diag(_pvalues)
   qvalues  = np.diag(_qvalues)
   rvalues = np.array([[_rvalues[0]]],dtype='float64')
  
   return beta,pvalues,qvalues,rvalues,state_dim

#Seasonality decomposition from sales data.
#Return: List of seasonality numbers.
def decomposition(observed,date):
   res = None
   try:
      data = [(date.iloc[i],observed[i][0]) for i in range(len(observed))]
      dd = pd.DataFrame(data,columns=["date","sls_q"])
      dd["date"] = pd.to_datetime(dd["date"])
      series = pd.Series(dd["sls_q"].values,index=dd["date"])
      res = sm.tsa.seasonal_decompose(series,model="additive")
      _min = abs(min(res.seasonal))
      for i in range(len(res.seasonal)):
         res.seasonal[i] = res.seasonal[i] + _min
   except:
      pass

   return res

#Saving output in hdfs model directory
def save(directory,item_id,unique_id,json_string):
   f = open("%s_params.txt"%item_id,"w")
   json.dump(json_string,f)
   f.close()
 
   #subprocess.call("hdfs dfs -mkdir -p '%s'/ckf_chain_model_zipped/'%s'/" % (directory, item_id),shell = True)
   subprocess.call("hdfs dfs -mkdir -p '%s'/'%s'/" % (directory, unique_id),shell = True)
   #subprocess.call("zip '%s'.zip $(ls '%s'_params.txt)> /dev/null" % (item_id,item_id), shell = True)
   subprocess.call("hdfs dfs -rm '%s'/'%s'/'%s'_params.txt" % (directory,unique_id,item_id), shell = True)
   #subprocess.call("hdfs dfs -put '%s'.zip '%s'/ckf_chain_model_zipped/'%s'/" % (item_id, directory, item_id), shell = True)
   subprocess.call("hdfs dfs -put '%s'_params.txt '%s'/'%s'/" % (item_id,directory,unique_id), shell = True)
   subprocess.call("rm %s_params.txt"%item_id, shell = True)
   #subprocess.call("rm %s.zip"%item_id, shell = True)


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

#Method for NULL price imputation in training data.
#Imputed prices are med_own_retl_a,med_reg_retl_a,retl_a
def nullPriceImputation(data):
   med_own_retl_a_list = data["med_own_retl_a"].tolist()
   med_reg_retl_a_list = data["med_reg_retl_a"].tolist()
   retl_a_list = data["retl_a"].tolist()
   own_ra_prev=0
   own_ra_curr=0
   reg_ra_prev=0
   reg_ra_curr=0
   ra_prev=0
   ra_curr=0
   for i in range(len(retl_a_list)):
       own_ra_curr = med_own_retl_a_list[i]
       reg_ra_curr = med_reg_retl_a_list[i]
       ra_curr = retl_a_list[i]
       if own_ra_curr == '\N':
          med_own_retl_a_list[i]=own_ra_prev
          own_ra_curr = own_ra_prev
       if reg_ra_curr == '\N':
          med_reg_retl_a_list[i]=reg_ra_prev
          reg_ra_curr = reg_ra_prev
       if ra_curr == '\N':
          retl_a_list[i]=ra_prev
          ra_curr = ra_prev
       own_ra_prev = own_ra_curr
       reg_ra_prev = reg_ra_curr
       ra_prev = ra_curr
   data["med_own_retl_a"]=med_own_retl_a_list
   data["med_reg_retl_a"]=med_reg_retl_a_list
   data["retl_a"]=retl_a_list

#Method for Price Imputation for future base price
#median = median(med_own_retl_a_last,med_reg_retl_a_last,retl_a_last)
#If the last value of retl_a is > 0.1 then future_base_price = median
#else: future_base_price=last med_own_retl_a
def priceImputation(data):
   med_own_retl_a_last = float(data["med_own_retl_a"].iloc[-1])
   med_reg_retl_a_last = float(data["med_reg_retl_a"].iloc[-1])
   retl_a_last         = float(data["retl_a"].iloc[-1])

   median = np.median([med_own_retl_a_last,med_reg_retl_a_last,retl_a_last])

   future_base_price = 0
   if retl_a_last > 0.1:
      future_base_price = median
   else:
      future_base_price = med_own_retl_a_last
   return future_base_price


#Main Method for training.
def run(data,directory="test"):
   #Null price imputation function call
   nullPriceImputation(data)

   #Cleanup of input data replacing \N with 0 value
   data.replace("\N", 0, inplace=True)

   #Parse Data frame function call
   df_date,df_obv,df_inp,datalen,item_id,dept_id,modelName,obv_var,beta_value,p_value,q_value,r_value,df_nstores,df_unique_id = parseDataFrame(data)
   
   #Fetching a model 
   model = fetch_model(modelName,beta_value,p_value,q_value,r_value)

   #Fetching a model parameter values.
   beta,pvalues,qvalues,rvalues,state_dim = parseModelFrame(model)

   date_format = pd.to_datetime(df_date)

   '''
   #extracting max values of retl_a,max_promo_dollaroff
   max_retl_a=df_inp['med_own_retl_a'].max()

   #normalize retl_a
   if (max_retl_a!=0):
      df_inp['med_own_retl_a']=df_inp['med_own_retl_a']/max_retl_a
   '''

   #Creating a list of observation and computing max value.
   observation = []
   max_obv = 0
   for i in range(datalen):
      observation.append([df_obv.iloc[i]])
      if df_obv.iloc[i]>max_obv:
         max_obv=df_obv.iloc[i]

   obv = observation

   #return if sales history <= 4weeks
   if len(obv) <4:
      return

   #seasonal decomposition
   res = decomposition(obv,df_date)

   #Week until holidays computation
   holidays = ["christmas_flag","newyearsday_flag"]
   weekUntilHolidays(data,holidays)

   latestDateTime = dt.datetime.strptime(df_date.iloc[datalen-1],"%Y-%m-%d")

   #Seasonal Map creation from seasonality extracted.
   #Map: key = week_end_date , value = seasonal number
   seas_map = {}
   if res:
      for i in range(len(obv)):
         date_str = df_date.iloc[i]
         date = dt.datetime.strptime(date_str,"%Y-%m-%d")

         if (latestDateTime-date).days <= 365:
            #date = date-dt.timedelta(days=365)
            date = dt.datetime.strftime(date,"%Y-%m-%d")
            key = date

            val = res.seasonal[i]

            if np.isnan(val):
               seas_map[key] = 0.0
            else:
               seas_map[key] = val

   #creating a list of indepenendent variables
   covariates = []
   for i in range(datalen):
  
      inputs = df_inp.iloc[i].tolist()

      if res:
         if np.isnan(res.seasonal[i]):
            seas = 0.0
         else:
            seas = res.seasonal[i]
      else:
         seas = 0.0

      #extracted seasonality 
      inputs.append(seas)

      #intercept
      inputs.append(1)

      ################# Experimentation #################
      #adding weekuntil vars
      #for hols in holidays:
      #   holsDframe = data["%s_until"%hols]
      #   inputs.append(holsDframe.iloc[i])
      ################# Experimentation #################
 
      covariates.append(np.array(inputs))


   cov = np.array(covariates)

   #ols coefficient for kf initials
   try:
      coeff,intercept = ols(cov,obv)
      beta_ = coeff.tolist()
      beta = beta_
   except:
      pass

   #calling bfgs optimizer to minimize log-likelihood
   x,f,d = fmin_l_bfgs_b(ckf,beta,args=(cov,obv,pvalues,qvalues,rvalues,0),approx_grad=True,maxiter=100)

   #running ckf on optimized point
   state,variance,likelihood_sum = ckf(x,cov,obv,pvalues,qvalues,rvalues,1)

   #preserving model and seas_map into hdfs
   beta = state[-1]

   beta_list = []
   for item in beta:
       beta_list.append(item[0])

   #creating sales vs week_end_date tuple list
   salesVsDate = []
   for i in range(datalen):
      salesVsDate.append((obv[i],df_date.iloc[i]))

   #Imputing the future base price value
   #future_base_price = priceImputation(data) 
   json_string = {"item_id":item_id,"unique_id":df_unique_id,"state":beta_list,"seasonal":seas_map,"max_retl_a":1.0,"future_base_price":1.0, "max_obv":max_obv,"sales":salesVsDate[-52:]}

   #saving to hdfs directory
   #save(directory,item_id,df_unique_id,json_string)
   return json_string,item_id,df_unique_id
