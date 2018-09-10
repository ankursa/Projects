USE ${hivevar:SCORING_DB};
SET hive.execution.engine=tez;

-- External table to add table structure to forecasts
CREATE EXTERNAL TABLE IF NOT EXISTS temp_mem_score_fcst (
 mdse_item_i           INT
,forecast_date         DATE         COMMENT "YYYY-MM-DD"
,store_count           SMALLINT     COMMENT "number of stores in LOCATION"
,forecast_q            FLOAT        COMMENT "base+adjustment"
)                                   COMMENT "Forecast data used by DFE API"
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS TEXTFILE LOCATION '${hivevar:SCORING_DB_DIR}/ScoreHub/fcsts';
