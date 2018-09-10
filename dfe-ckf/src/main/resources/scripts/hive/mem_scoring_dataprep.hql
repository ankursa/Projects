CREATE DATABASE IF NOT EXISTS ${hivevar:SCORING_DB} LOCATION '${hivevar:SCORING_DB_DIR}';
use ${hivevar:SCORING_DB};

SET hive.execution.engine=tez;
SET mapreduce.job.reduces=-1;
SET mapreduce.input.fileinputformat.split.maxsize          = 2147483648;
SET mapreduce.input.fileinputformat.split.minsize          = 1073741824;
SET mapreduce.input.fileinputformat.split.minsize.per.node = 1073741824;
SET mapreduce.input.fileinputformat.split.minsize.per.rack = 1073741824;
SET hive.exec.max.dynamic.partitions=100000;
SET hive.exec.max.dynamic.partitions.pernode=100000;
SET hive.vectorized.execution.enabled = true;
SET hive.vectorized.execution.reduce.enabled = true;
SET hive.exec.orc.split.strategy=BI;

--MEM Score Item Universe - Items should be STORE_ONLY or CROSS OVER and
-- Non-New Items(Sales history should be more than 4 weeks)

DROP TABLE IF EXISTS temp_mem_score_item_universe;
CREATE TABLE temp_mem_score_item_universe
STORED AS ORC AS
    SELECT A.mdse_item_i,
            A.dpci_lbl_t,
            A.ecom_item_i,
            A.mzrt_item_i,
            B.mdse_div_ref_i,
            B.mdse_dept_ref_i,
            B.mdse_sbcl_ref_i,
            B.mdse_sbcl_i
    FROM
    (SELECT  mdse_item_i,dpci_lbl_t,ecom_item_i,mzrt_item_i
     FROM
        (SELECT mdse_item_i,dpci_lbl_t,ecom_item_i,mzrt_item_i,
                ROW_NUMBER() OVER (PARTITION BY mdse_item_i ORDER BY ecom_eff_d desc) AS r
         FROM ${hivevar:SLS_HIST_DB}.master_item_mapping
            WHERE item_type in ('STORE_ONLY','CROSSOVER') AND NEW_ITEM='N'
        ) S
     WHERE S.r = 1
    ) A
    LEFT JOIN
    ${hivevar:SLS_HIST_DB}.department_item_list B
    ON A.mdse_item_i=B.mdse_item_i
;

-- Identifying the list of items group by mdse_div_ref_i, mdse_dept_ref_i,mdse_sbcl_ref_i ,so that we can split
-- key to subkeys based on no of items for given key.
DROP TABLE IF EXISTS temp_key_level_items_cnt;
CREATE  TABLE temp_key_level_items_cnt
STORED AS ORC AS
SELECT  mdse_div_ref_i, mdse_dept_ref_i,mdse_sbcl_ref_i,count(distinct mdse_item_i) items_cnt
FROM temp_mem_score_item_universe
GROUP BY mdse_div_ref_i, mdse_dept_ref_i,mdse_sbcl_ref_i;

-- Scoring data prep for MEM
DROP TABLE IF EXISTS temp_mem_scoring_data_prep;
CREATE TABLE temp_mem_scoring_data_prep(
    mdse_item_i                 int             COMMENT 'The data warehouse system generated surrogate key for a Merchandise Item.',
    holiday_array               array<string>   COMMENT 'an array holiday strings for a given week from Sun to Sat',
    n_stores                    int             COMMENT 'Number of unique stores',
    fcg_q                       double          COMMENT 'The number of facings for this item. Facing is the vertical or horizontal number of items on a planogrammed display. On non-retail items, this is the number of units required to build the display.',
    retl_a                      double          COMMENT 'Regular sales total retail dollars.',
    max_promo_daynr             double          COMMENT 'maximum numbers of days for items on promotion',
    max_promo_pctoff            double          COMMENT 'maximum percent off for given promotions',
    max_promo_dollaroff         double          COMMENT 'maximum dollar-off for given promotions',
    max_external_pctoff         double          COMMENT 'maximum percent off for external promotions',
    max_external_dollaroff      double          COMMENT 'maximum dollar off for external promotions',
    circular_flag               double          COMMENT 'avg on daily circular promotion indicators',
    circular_flag_count         int             COMMENT 'sum of daily circular promotion indicators',
    tpc_flag                    double          COMMENT 'avg on TPC promotion indicators',
    tpc_flag_count              int             COMMENT 'sum of TPC promotion indicators',
    clearance_flag              double          COMMENT 'avg on clearance indicators',
    clearance_flag_count        int             COMMENT 'sum of clearance indicators',
    dollar_off_flag             double          COMMENT 'avg on flags for dollar off',
    dollar_off_flag_count       int             COMMENT 'sum of flags for dollar off',
    qty_for_dollar_flag         double          COMMENT 'avg on quantity flags for dollar-off',
    qty_for_dollar_flag_count   int             COMMENT 'sum of quantity flags for dollar-off',
    free_product_flag           double          COMMENT 'avg on flags for free product',
    free_product_flag_count     int             COMMENT 'sum of flags for free product',
    pct_off_flag                double          COMMENT 'avg on flags for free product',
    pct_off_flag_count          int             COMMENT 'sum of flags for percent off',
    giftcard_flag               double          COMMENT 'avg on flags for giftcards',
    giftcard_flag_count         int             COMMENT 'sum of flags for giftcards',
    external_promo_flag         double          COMMENT 'avg on flags for external promotions',
    external_promo_flag_count   int             COMMENT 'sum of flags for external promotions',
    pictured_flag               double          COMMENT 'avg on flags for pictured items on circular promotion',
    pictured_flag_count         int             COMMENT 'sum of flags for pictured items on circular promotion',
    min_pagenumber              double          COMMENT 'min_pagenumber',
    week_start_date             string          COMMENT 'week start date',
    week_end_date               string          COMMENT 'week end date',
    rel_d                       string          COMMENT 'rel_d'
)
PARTITIONED BY (key string COMMENT 'key div_dept_sbcls')
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|';

--- Retrieving store forward predictors information for items defined for MEM score item universe
-- and construct key - div + dept + sbcls
-- few keys are having huge number of items, hence processing take long time, so splitting the bigger key into
-- smaller keys(key + sub_classi) by having configurable no of items per key.
INSERT INTO TABLE temp_mem_scoring_data_prep PARTITION(key)
    SELECT A.mdse_item_i,
           A.holiday_array,
           A.n_stores,
           A.fcg_q,
           A.retl_a,
           A.max_promo_daynr,
           A.max_promo_pctoff,
           A.max_promo_dollaroff,
           A.max_external_pctoff,
           A.max_external_dollaroff,
           A.circular_flag,
           A.circular_flag_count,
           A.tpc_flag,
           A.tpc_flag_count,
           A.clearance_flag,
           A.clearance_flag_count,
           A.dollar_off_flag,
           A.dollar_off_flag_count,
           A.qty_for_dollar_flag,
           A.qty_for_dollar_flag_count,
           A.free_product_flag,
           A.free_product_flag_count,
           A.pct_off_flag,
           A.pct_off_flag_count,
           A.giftcard_flag,
           A.giftcard_flag_count,
           A.external_promo_flag,
           A.external_promo_flag_count,
           A.pictured_flag,
           A.pictured_flag_count,
           A.min_pagenumber,
           A.week_start_date,
           A.week_end_date,
           A.rel_d,
            IF(items_cnt < ${hivevar:SPLIT_ITEM_CNT},
            concat(B.mdse_div_ref_i, "_", B.mdse_dept_ref_i, "_",B.mdse_sbcl_ref_i) ,
            concat(B.mdse_div_ref_i, "_", B.mdse_dept_ref_i, "_",B.mdse_sbcl_ref_i,"_",B.mdse_sbcl_i) ) as key
    FROM ${hivevar:SLS_HIST_DB}.store_forward_predictors_chain_week  A
    INNER JOIN temp_mem_score_item_universe B
    ON A.mdse_item_i=B.mdse_item_i
    INNER JOIN temp_key_level_items_cnt C
    ON B.mdse_div_ref_i=C.mdse_div_ref_i
            AND B.mdse_dept_ref_i=C.mdse_dept_ref_i
            AND B.mdse_sbcl_ref_i=C.mdse_sbcl_ref_i
    DISTRIBUTE BY key
;

-- fetching list of distinct stores
DROP TABLE IF EXISTS temp_loc_list;
CREATE TABLE temp_loc_list
AS
SELECT DISTINCT co_loc_i FROM ${hivevar:SLS_HIST_DB}.store_sales_history_loc_week;


-- Generating promotion list for scoring item universe
DROP TABLE IF EXISTS temp_agg_promo_list;
CREATE TABLE temp_agg_promo_list(
promotionid     string      COMMENT 'WCS Promotion Identifier',
mdse_item_i     int         COMMENT 'The data warehouse system generated surrogate key for a Merchandise Item.',
start_date      string      COMMENT 'Identifies start date of promotion campaign',
end_date        string      COMMENT 'Identifies end date of promotion campaign',
vehicle         string      COMMENT 'Identifies the vehicle code.',
rewardtype      string      COMMENT 'Identifies the reward types',
rewardvalue     double      COMMENT 'Identifies the reward values',
storeCount      bigint      COMMENT 'storeCount'
)
PARTITIONED BY (key string COMMENT 'key div_dept_sbcls')
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|';

-- Gathering future promotions for Mem Score Item Universe of valid stores
-- end_date should correspond to most recent Sunday since model

INSERT INTO TABLE temp_agg_promo_list PARTITION(key)
SELECT  A.promotionid,
        A.mdse_item_i,
        start_date,
        end_date,
        vehicle,
        rewardtype,
        rewardvalue,
        COUNT(DISTINCT A.co_loc_i) as storeCount,
        key
FROM	${hivevar:SLS_HIST_DB}.promo_list A
INNER JOIN
(   SELECT promotionid, MAX(lastmodifieddate) as max_lastmodifieddate
    FROM ${hivevar:SLS_HIST_DB}.promo_list
    GROUP BY promotionid
) as B
    ON A.promotionid = B.promotionid AND A.lastmodifieddate = B.max_lastmodifieddate
INNER JOIN  temp_loc_list C
ON	A.co_loc_i = C.co_loc_i
INNER JOIN (select distinct mdse_item_i,key from temp_mem_scoring_data_prep ) D
ON A.mdse_item_i=D.mdse_item_i
WHERE	end_date >= date_sub('${hivevar:RELEASE_DATE}',pmod(datediff('${hivevar:RELEASE_DATE}','1900-01-07'),7))
GROUP BY A.promotionid, A.mdse_item_i, start_date, end_date, vehicle, rewardtype, rewardvalue,key
DISTRIBUTE BY key;
