SET hive.exec.dynamic.partition.mode=nonstrict;
SET hive.exec.max.dynamic.partitions.pernode=10000;
SET hive.exec.max.dynamic.partitions=10000;
set hive.execution.engine=tez;

CREATE DATABASE IF NOT EXISTS ${hivevar:CKF_DB} LOCATION '${hivevar:CKF_DB_DIR}';

create external table if not exists ${hivevar:CKF_DB}.${hivevar:SCORE_OUTPUT_TABLE} (mdse_item_i int, mdse_dept_ref_i int, week_end_date string, predicted float)
ROW FORMAT delimited fields terminated by ' '
-- EEFE-2896
STORED AS TEXTFILE
location '${hivevar:SCORE_OUTPUT_DIR}';

INSERT OVERWRITE TABLE ${hivevar:MODEL_LANDING_TABLE}
PARTITION (forecast_release_date='${hivevar:RELEASE_DATE}',
forecast_granularity=7, model_id=${hivevar:MODELID}, location_type=1)
SELECT
a.mdse_item_i
, NULL
, NULL
, c.dpci_lbl_t AS dpci
, date_sub(a.week_end_date,6) AS forecast_date
, NULL
, b.n_stores as store_count
, predicted/b.n_stores AS forecast_q
from ${hivevar:CKF_DB}.${hivevar:SCORE_OUTPUT_TABLE} as a
INNER JOIN ${hivevar:SALES_FORECAST_TABLE} as b
ON a.mdse_item_i=b.mdse_item_i
AND a.week_end_date=b.week_end_date
INNER JOIN (select distinct t2.mdse_item_i, t2.dpci_lbl_t from ${hivevar:MASTER_ITEM_MAPPING_TABLE} t2 where t2.item_type <> 'WEB_ONLY' and t2.new_item = 'N') c
ON a.mdse_item_i=c.mdse_item_i
WHERE a.mdse_item_i IS NOT NULL
AND predicted IS NOT NULL;
