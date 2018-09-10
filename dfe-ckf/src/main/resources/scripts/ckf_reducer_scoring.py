#!/usr/local/python-tgt/bin/python2.7

from operator import itemgetter
import sys
import pandas as pd
import subprocess
sys.path.append(".")
from algo_integration_scoring import run

current_item = None
current_count = 0
item = None
#header_row = ["mdse_item_i","mdse_clas_i","mdse_dept_ref_i","week_start_date","week_end_date","sls_retl_a","retl_a","sls_unit_q","avg_sales","n_stores","circular_flag","circular_flag_count","clearance_flag","clearance_flag_count","dollar_off_flag","dollar_off_flag_count","tpc_flag","tpc_flag_count","pct_off_flag","pct_off_flag_count","christmas_flag","easter_flag","thanksgiving_flag","newyearsday_flag","fathersday_flag","mothersday_flag","julyfourth_flag","valentines_flag","memorialday_flag","halloween_flag","superbowl_flag","stpatricks_flag","ashwednesday_flag","blackfriday_flag","columbusday_flag","cybermonday_flag","goodfriday_flag","holysaturday_flag","laborday_flag","mardigras_flag","mlkday_flag","presidentsday_flag","veteransday_flag","all_holidays","wk1_flag","wk2_flag","wk3_flag","wk4_flag","wk5_flag","wk6_flag","wk7_flag","wk8_flag","wk9_flag","wk10_flag","xmas1_flag","xmas2_flag","unique_id"]

header_row = ["mdse_item_i","mdse_clas_i","mdse_dept_ref_i","week_start_date","week_end_date","sls_retl_a","med_own_retl_a","sls_unit_q","avg_sales","n_stores","circular_flag","circular_flag_count","clearance_flag","clearance_flag_count","dollar_off_flag","dollar_off_flag_count","tpc_flag","tpc_flag_count","pct_off_flag","pct_off_flag_count","max_promo_daynr","max_promo_dollaroff","max_external_pctoff","christmas_flag","easter_flag","thanksgiving_flag","newyearsday_flag","fathersday_flag","mothersday_flag","julyfourth_flag","valentines_flag","memorialday_flag","halloween_flag","superbowl_flag","stpatricks_flag","ashwednesday_flag","blackfriday_flag","columbusday_flag","cybermonday_flag","goodfriday_flag","holysaturday_flag","laborday_flag","mardigras_flag","mlkday_flag","presidentsday_flag","veteransday_flag","all_holidays","wk1_flag","wk2_flag","wk3_flag","wk4_flag","wk5_flag","wk6_flag","wk7_flag","wk8_flag","wk9_flag","wk10_flag","xmas1_flag","xmas2_flag","unique_id"]

current_data = []
current_row = []

modelPath=sys.argv[1]

def loadModelParams(item_id,unique_id,directory='test'):
    json_str = None
    try:
       #subprocess.call("hdfs dfs -copyToLocal '%s'/ckf_chain_model_zipped/'%s'/'%s'.zip ./" % (directory,item_id,item_id), shell = True)  
       subprocess.call("hdfs dfs -copyToLocal '%s'/%s.json ./" % (directory,unique_id), shell = True)  
       #subprocess.call("unzip %s.zip > /dev/null" % (item_id),shell = True)
       #subprocess.call("unzip %s.zip > /dev/null" % (unique_id),shell = True)
       #subprocess.call("unzip %s/%s/%s.zip"%(unique_id,item_id,item_id))
       #f = open("%s/%s_params.txt" % (unique_id,item_id))
       f = open("%s.json" % (unique_id))
       json_str = f.readlines()[0]
       f.close()
       #subprocess.call("rm %s_params.txt %s.zip" % (item_id,item_id), shell=True)
       subprocess.call("rm %s.json" % (unique_id), shell=True)
    except:
       pass

    return json_str 


# input comes from STDIN
for line in sys.stdin:
    if len(line)==0:
       pass

    else:
       # remove leading and trailing whitespace
       line = line.strip()

       # parse the input we got from mapper.py
       item, cols = line.split('\t', 1)
       current_row = cols.split(',')
       # this IF-switch only works because Hadoop sorts map output
       # by key (here: word) before it is passed to the reducer
       if item == current_item:
          current_count += 1
          current_data.append(current_row)
       else:
          if current_item:
             #create dataframe from current_data and pass to ckf
             data = pd.DataFrame(current_data)
             data.columns=header_row
             data["week_end_date_new"] = pd.to_datetime(data["week_end_date"])
             data = data.sort_values(by="week_end_date_new")
             #running ckf model
             unique_id = data["unique_id"].tolist()[0]
             modelParams = loadModelParams(current_item,unique_id,modelPath)
             if modelParams is not None:
                run(data,modelParams)
          #resetting data dataframe
          current_data = []
          current_data.append(current_row)
          current_count = 1
          current_item = item

# do not forget to output for the last item!
if current_item == item and current_item != None and item != None:
    data = pd.DataFrame(current_data)
    data.columns=header_row
    data["week_end_date_new"] = pd.to_datetime(data["week_end_date"])
    data = data.sort_values(by="week_end_date_new")
    #running ckf mode
    unique_id = data["unique_id"].tolist()[0]
    modelParams = loadModelParams(current_item,unique_id,modelPath)
    if modelParams is not None:
       run(data,modelParams) 
    #print '%s\t%s' % (current_item, current_count)
