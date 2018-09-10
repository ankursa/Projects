CREATE DATABASE IF NOT EXISTS ${hivevar:TRAINING_DB} LOCATION '${hivevar:TRAINING_DB_DIR}';
use ${hivevar:TRAINING_DB};

SET hive.execution.engine=tez;
SET hive.exec.max.dynamic.partitions=100000;
SET hive.exec.max.dynamic.partitions.pernode=100000;
SET hive.vectorized.execution.enabled = true;
SET hive.vectorized.execution.reduce.enabled = true;

--MEM Item Universe-Items should be STORE_ONLY or CROSS OVER and Non-New Items(Sales history should be more than 4 weeks)
DROP TABLE IF EXISTS temp_mem_train_item_universe;
CREATE TABLE temp_mem_train_item_universe AS
    SELECT A.mdse_item_i,
            B.mdse_div_ref_i,
            B.mdse_dept_ref_i,
            B.mdse_sbcl_ref_i,
            B.mdse_sbcl_i
    FROM
    (SELECT DISTINCT mdse_item_i
     FROM ${hivevar:SLS_HIST_DB}.master_item_mapping
        WHERE item_type in ('STORE_ONLY','CROSSOVER') AND NEW_ITEM='N'
    ) A
    LEFT JOIN
    ${hivevar:SLS_HIST_DB}.department_item_list B
    ON A.mdse_item_i=B.mdse_item_i
;

-- Identifying the list of items group by mdse_div_ref_i, mdse_dept_ref_i,mdse_sbcl_ref_i ,so that we can split
-- key to subkeys based on no of items for given key.
DROP TABLE IF EXISTS temp_key_level_items_cnt;
CREATE  TABLE temp_key_level_items_cnt AS
    SELECT  mdse_div_ref_i, mdse_dept_ref_i,mdse_sbcl_ref_i,count(distinct mdse_item_i) items_cnt
    FROM temp_mem_train_item_universe
    GROUP BY mdse_div_ref_i, mdse_dept_ref_i,mdse_sbcl_ref_i;


-- Training data prep for MEM
DROP TABLE IF EXISTS temp_mem_train_data_prep;
CREATE TABLE temp_mem_train_data_prep(
    mdse_item_i                 int             COMMENT 'The data warehouse system generated surrogate key for a Merchandise Item.',
    holiday_array               array<string>   COMMENT 'an array holiday strings for a given week from Sun to Sat',
    sls_retl_a                  double          COMMENT 'A point-in-time snapshot of the merchant-established today retail price, charged by Target Corporation to its guests, to purchase a single unit of an item, prior to any store discretionary markdowns or transaction level discounts.',
    retl_a                      double          COMMENT 'Regular sales total retail dollars.',
    sls_unit_q                  int             COMMENT 'Number of units sold or refunded per line item.',
    n_stores                    double          COMMENT 'Number of unique stores',
    fcg_q                       double          COMMENT 'The number of facings for this item. Facing is the vertical or horizontal number of items on a planogrammed display. On non-retail items, this is the number of units required to build the display.',
    boh_q                       double          COMMENT 'The quantity of item units (eaches, pounds, yards, fluid ounces, etc) on hand at a store or warehouse at the Start of the day. This equals the ENDING_ON_HAND_QUANTITY/EOH_Q from the prior day.',
    clearance_pct               double          COMMENT 'clearance indicator: 1 items on clearance, 0 otherwise',
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
    pictured_flag_count         int             COMMENT 'sum of flags for pictured items on circular promotion',
    pictured_flag               double          COMMENT 'avg on flags for pictured items on circular promotion',
    min_pagenumber              double          COMMENT 'minimum page number for pictured circular items',
    stockout_flag_count         int             COMMENT 'sum of flags : 1 if sls_unit_q is NULL, 0 otherwise',
    stockout_flag               double          COMMENT 'avg on flags : 1 if sls_unit_q is NULL, 0 otherwise',
    week_start_date             string          COMMENT 'week start date',
    cpn_f                       double          COMMENT 'avg of cpn_f',
    cpn_f_count                 int             COMMENT 'sum of cpn_f',
    wt_cpn_f                    double          COMMENT 'Percentage of cpn_f, weighted by sls_unit_q',
    pmtn_item_f                 double          COMMENT 'avg of pmtn_item_f',
    pmtn_item_f_count           int             COMMENT 'sum of pmtn_item_f',
    wt_pmtn_item_f              double          COMMENT 'Percentage of pmtn_item_f, weighted by sls_unit_q',
    tpr_f                       double          COMMENT 'avg of tpr_f',
    tpr_f_count                 int             COMMENT 'sum of tpr_f',
    wt_tpr_f                    double          COMMENT 'Percentage of tpr_f, weighted by sls_unit_q',
    prc_guar_chit_f             double          COMMENT 'avg of prc_guar_chit_f',
    prc_guar_chit_f_count       int             COMMENT 'sum of prc_guar_chit_f',
    wt_prc_guar_chit_f          double          COMMENT 'Percentage of prc_guar_chit_f, weighted by sls_unit_q',
    circ_f                      double          COMMENT 'avg of circ_f',
    circ_f_count                int             COMMENT 'sum of circ_f',
    wt_circ_f                   double          COMMENT 'Percentage of circ_f, weighted by sls_unit_q',
    n_stores_asmt               double          COMMENT 'the number of stores in which the item was in the assortment (slated to be sold/on the shelves) on a particular day.',
    n_stores_sales              double          COMMENT 'The total number of currently open distinct stores in which the item had at least one sale over a particular day (for daily table) or week (for weekly table)',
    med_own_retl_a              double          COMMENT 'Median over own_retl_a across all stores. See source table prd_ssl_fnd.mdse_slstr_item_line for details on own_retl_a.',
    med_reg_retl_a              double          COMMENT 'Median over reg_retl_a across all stores. See source table prd_ssl_fnd.mdse_slstr_item_line for details on reg_retl_a.',
    med_customer_paid_price     double          COMMENT 'Median over ext_sls_prc_a across all stores. See source table prd_ssl_fnd.mdse_slstr_item_line for details on ext_sls_prc_a.',
    wt_own_retl_a               double          COMMENT 'Average of own_retl_a across all days in the week and all stores, weighted by quantity sls_unit_q.',
    wt_reg_retl_a               double          COMMENT 'Average of reg_retl_a across all days in the week and all stores, weighted by quantity sls_unit_q.',
    wt_customer_paid_price      double          COMMENT 'Average of ext_sls_prc_a across all days in the week and all stores, weighted by quantity sls_unit_q.',
    week_end_date               string          COMMENT 'week end date'
)
PARTITIONED BY (key string COMMENT 'key div_dept_sbcls/div_dept_sbclsrefi_sbcls_name')
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|';

--- Retrieving store sales history information for items defined for MEM universe
-- and construct key - div + dept + sbcls
-- few keys are having huge number of items, hence processing take long time, so splitting the bigger key into
-- smaller keys(key + sub_classi) by having configurable no of items per key.
INSERT INTO TABLE temp_mem_train_data_prep PARTITION(key)
    SELECT  A.mdse_item_i,
            A.holiday_array,
            A.sls_retl_a,
            A.retl_a,
            A.sls_unit_q,
            A.n_stores,
            A.fcg_q,
            A.boh_q,
            A.clearance_pct,
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
            A.pictured_flag_count,
            A.pictured_flag,
            A.min_pagenumber,
            A.stockout_flag_count,
            A.stockout_flag,
            A.week_start_date,
            A.cpn_f,
            A.cpn_f_count,
            A.wt_cpn_f,
            A.pmtn_item_f,
            A.pmtn_item_f_count,
            A.wt_pmtn_item_f,
            A.tpr_f,
            A.tpr_f_count,
            A.wt_tpr_f,
            A.prc_guar_chit_f,
            A.prc_guar_chit_f_count,
            A.wt_prc_guar_chit_f,
            A.circ_f,
            A.circ_f_count,
            A.wt_circ_f,
            A.n_stores_asmt,
            A.n_stores_sales,
            A.med_own_retl_a,
            A.med_reg_retl_a,
            A.med_customer_paid_price,
            A.wt_own_retl_a,
            A.wt_reg_retl_a,
            A.wt_customer_paid_price,
            A.week_end_date,
            IF(items_cnt < ${hivevar:SPLIT_ITEM_CNT},
            concat(B.mdse_div_ref_i, "_", B.mdse_dept_ref_i, "_",B.mdse_sbcl_ref_i) ,
            concat(B.mdse_div_ref_i, "_", B.mdse_dept_ref_i, "_",B.mdse_sbcl_ref_i,"_",B.mdse_sbcl_i) ) as key
    FROM ${hivevar:SLS_HIST_DB}.store_sales_history_chain_week A
    INNER JOIN temp_mem_train_item_universe B
    ON A.mdse_item_i=B.mdse_item_i
    INNER JOIN temp_key_level_items_cnt C
    ON B.mdse_div_ref_i=C.mdse_div_ref_i
        AND B.mdse_dept_ref_i=C.mdse_dept_ref_i
        AND B.mdse_sbcl_ref_i=C.mdse_sbcl_ref_i
    WHERE A.week_end_date < CURRENT_DATE
    DISTRIBUTE BY key
;

--Generating subgroups which will be used in training and scoring
DROP TABLE IF EXISTS temp_mem_subgroups;
CREATE TABLE temp_mem_subgroups(
    mdse_item_i     int     COMMENT 'The data warehouse system generated surrogate key for a Merchandise Item.',
    mdse_sbcl_ref_i int     COMMENT 'Represents merchandise subclass',
    mdse_sbcl_i     string  COMMENT 'The name of the sub class'
)
PARTITIONED BY (key string COMMENT 'key div_dept_sbcls')
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|';

-- Generating subgroups - will be used in scoring as metadata for keys
INSERT INTO TABLE temp_mem_subgroups PARTITION(key)
    SELECT DISTINCT A.mdse_item_i,
                    B.mdse_sbcl_ref_i,
                    B.mdse_sbcl_i,
                    key
    FROM temp_mem_train_data_prep A
    INNER JOIN temp_mem_train_item_universe B
    ON A.mdse_item_i=B.mdse_item_i
   DISTRIBUTE BY key
;

SET mapreduce.job.reduces=1;
-- storing distinct keys in one table
DROP TABLE IF EXISTS temp_model_keys;
CREATE TABLE temp_model_keys
ROW FORMAT DELIMITED
FIELDS TERMINATED BY  ","
AS
SELECT DISTINCT key from temp_mem_subgroups ORDER BY key;
