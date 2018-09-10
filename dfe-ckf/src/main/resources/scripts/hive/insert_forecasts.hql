SET hive.execution.engine=tez;
SET hive.vectorized.execution.enabled = true;
SET hive.vectorized.execution.reduce.enabled = true;
SET hive.exec.orc.split.strategy=BI;
SET hive.exec.dynamic.partition.mode=nonstrict;
SET hive.exec.max.dynamic.partitions.pernode=10000;
SET hive.exec.max.dynamic.partitions=10000;

USE ${hivevar:PRD_FCST_DB};

-- More info of below table is available at https://confluence.target.com/pages/viewpage.action?spaceKey=EDDF&title=DFE+Forecast+Data+table+schema

CREATE EXTERNAL TABLE IF NOT EXISTS dfe_forecasts (
 mdse_item_i           INT
,ecom_item_i           INT
,tcin                  INT
,dpci                  STRING       COMMENT "DDD-CC-IIII"
,forecast_date         DATE         COMMENT "YYYY-MM-DD"
,location              INT          COMMENT "see column LOCATION_TYPE"
,store_count           SMALLINT     COMMENT "number of stores in LOCATION"
,forecast_q            FLOAT        COMMENT "base+adjustment"
)                                   COMMENT "Forecast data used by DFE API"
PARTITIONED BY (
 forecast_release_date DATE         COMMENT "YYYY-MM-DD"
,forecast_granularity SMALLINT      COMMENT "number of days"
,model_id TINYINT                   COMMENT "see table FORECAST_MODEL_NAMES"
,location_type TINYINT              COMMENT "see table FORECAST_LOCATION_TYPES"
)
STORED AS ORC tblproperties ('orc.compression'='SNAPPY');

INSERT OVERWRITE TABLE dfe_forecasts
PARTITION (forecast_release_date='${hivevar:RELEASE_DATE}',
forecast_granularity=7, model_id=${hivevar:MODEL_ID}, location_type=1)
SELECT
a.mdse_item_i
, ecom_item_i
, mzrt_item_i AS tcin
, dpci_lbl_t AS dpci
, forecast_date
, NULL
, store_count
, forecast_q
FROM ${hivevar:SCORING_DB}.temp_mem_score_fcst as a
INNER JOIN ${hivevar:SCORING_DB}.temp_mem_score_item_universe as b
ON a.mdse_item_i=b.mdse_item_i;
