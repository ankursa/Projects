set hive.execution.engine=tez;
set hive.auto.convert.join=false;

CREATE DATABASE IF NOT EXISTS ${hivevar:CKF_DB} LOCATION '${hivevar:CKF_DB_DIR}';

drop table if exists ${hivevar:CKF_DB}.temp_training_data;
create table ${hivevar:CKF_DB}.temp_training_data as select t1.mdse_item_i, t1.mdse_clas_i, mdse_dept_ref_i, week_start_date, t1.week_end_date as week_end_date, sls_retl_a, retl_a, med_own_retl_a, med_reg_retl_a, med_customer_paid_price, sls_unit_q, sls_unit_q/coalesce(n_stores_asmt,n_stores) as avg_sales, coalesce(n_stores_asmt,n_stores) as n_stores, n_stores_asmt, circular_flag, circular_flag_count, clearance_flag, clearance_flag_count, dollar_off_flag, dollar_off_flag_count, tpc_flag, tpc_flag_count, pct_off_flag, pct_off_flag_count,max_promo_daynr,max_promo_dollaroff,max_external_pctoff,
if(array_contains(holiday_array,'christmas'),1,0) christmas_flag,
if(array_contains(holiday_array,'easter'),1,0) easter_flag,
if(array_contains(holiday_array,'thanksgiving'),1,0) thanksgiving_flag,
if(array_contains(holiday_array,'newyearsday'),1,0) newyearsday_flag,
if(array_contains(holiday_array,'fathersday'),1,0) fathersday_flag,
if(array_contains(holiday_array,'mothersday'),1,0) mothersday_flag,
if(array_contains(holiday_array,'julyfourth'),1,0) julyfourth_flag,
if((array_contains(holiday_array,'valentines') OR t1.week_end_date='2018-02-17'),1,0) valentines_flag,
if(array_contains(holiday_array,'memorialday'),1,0) memorialday_flag,
if(array_contains(holiday_array,'halloween'),1,0) halloween_flag,
if(array_contains(holiday_array,'superbowl'),1,0) superbowl_flag,
if(array_contains(holiday_array,'stpatricks'),1,0) stpatricks_flag,
if(array_contains(holiday_array,'ashwednesday'),1,0) ashwednesday_flag,
if(array_contains(holiday_array,'black.friday'),1,0) blackfriday_flag,
if(array_contains(holiday_array,'columbusday'),1,0) columbusday_flag,
if(array_contains(holiday_array,'cyber.monday'),1,0) cybermonday_flag,
if(array_contains(holiday_array,'goodfriday'),1,0) goodfriday_flag,
if(array_contains(holiday_array,'holysaturday'),1,0) holysaturday_flag,
if(array_contains(holiday_array,'laborday'),1,0) laborday_flag,
if(array_contains(holiday_array,'mardigras'),1,0) mardigras_flag,
if(array_contains(holiday_array,'mlkday'),1,0) mlkday_flag,
if(array_contains(holiday_array,'presidentsday'),1,0) presidentsday_flag,
if(array_contains(holiday_array,'veteransday'),1,0) veteransday_flag,
if(array_contains(holiday_array, 'newyearsday') or array_contains(holiday_array, 'fathersday') or array_contains(holiday_array, 'mothersday') or array_contains(holiday_array, 'valentines') or array_contains(holiday_array, 'superbowl') or array_contains(holiday_array, 'stpatricks') or array_contains(holiday_array, 'memorialday') or array_contains(holiday_array, 'halloween') or array_contains(holiday_array, 'julyfourth') 
or array_contains(holiday_array, 'ashwednesday') or array_contains(holiday_array, 'columbusday') or array_contains(holiday_array, 'holysaturday') or array_contains(holiday_array, 'laborday') or array_contains(holiday_array, 'mardigras') or array_contains(holiday_array, 'mlkday') or array_contains(holiday_array, 'presidentsday') or array_contains(holiday_array, 'veteransday') , 1, 0) all_holidays,
COALESCE(wk1_flag, 0) AS wk1_flag
,COALESCE(wk2_flag, 0) AS wk2_flag
,COALESCE(wk3_flag, 0) AS wk3_flag
,COALESCE(wk4_flag, 0) AS wk4_flag
,COALESCE(wk5_flag, 0) AS wk5_flag
,COALESCE(wk6_flag, 0) AS wk6_flag
,COALESCE(wk7_flag, 0) AS wk7_flag
,COALESCE(wk8_flag, 0) AS wk8_flag
,COALESCE(wk9_flag, 0) AS wk9_flag
,COALESCE(wk10_flag, 0) AS wk10_flag
,COALESCE(xmas1_flag, 0) AS xmas1_flag
,COALESCE(xmas2_flag, 0) AS xmas2_flag
from ${hivevar:SALES_HISTORY_TABLE} t1
INNER JOIN (select distinct t4.mdse_item_i, t4.dpci_lbl_t from ${hivevar:MASTER_ITEM_MAPPING_TABLE} t4 where t4.item_type <> 'WEB_ONLY' AND t4.new_item = 'N') t5 ON t1.mdse_item_i=t5.mdse_item_i
LEFT OUTER JOIN ${hivevar:CALENDAR_BTS_TABLE} t2 on t1.week_end_date=t2.week_end_date 
LEFT OUTER JOIN ${hivevar:CALENDAR_CHRISTMAS_TABLE} t3 on t1.week_end_date=t3.week_end_date
where t1.week_end_date<current_date;

drop table if exists ${hivevar:CKF_DB}.temp_item_id_map;
create table ${hivevar:CKF_DB}.temp_item_id_map as select t1.mdse_item_i, floor(rand()*int(${hivevar:NUM_UNIQUE_IDS})) as unique_id from (select distinct mdse_item_i from ${hivevar:CKF_DB}.temp_training_data) as t1;

drop table if exists ${hivevar:CKF_DB}.${hivevar:TRAIN_INPUT_TABLE};
create table ${hivevar:CKF_DB}.${hivevar:TRAIN_INPUT_TABLE}
-- EEFE-2896
STORED AS TEXTFILE as
select t1.*,t2.unique_id from ${hivevar:CKF_DB}.temp_training_data t1 INNER JOIN ${hivevar:CKF_DB}.temp_item_id_map t2
ON t1.mdse_item_i = t2.mdse_item_i;

drop table ${hivevar:CKF_DB}.temp_training_data;
