set -vx

HIVE_FCST_DB=$1
QUEUE_NAME=$2
SCORE_OUTPUT_TABLE=$3
SCORE_INPUT_TABLE=$4

HIVE_ENGINE=tez


echo "<!DOCTYPE html>"
echo "<html>"
echo "<head>"
echo "<title>[DFE] CKF-Forecasting QA Report</title>"

echo "<style type=text/css>"

echo "table {"
echo "  border-collapse: separate;"
echo "  border-spacing: 0;"
echo "  color: #4a4a4d;"
echo "  font: 14px/1.4 "Helvetica Neue", Helvetica, Arial, sans-serif;"
echo "}"
echo "th,"

echo "td {"
echo "  padding: 10px 15px;"
echo "  vertical-align: middle;"
echo "}"

echo "thead {"
echo "  background: #395870;"
echo "  color: #fff;"
echo "}"
echo "th {"
echo "  font-weight: bold;"
echo "}"
echo "th:first-child {"
echo "  text-align: left;"
echo "}"
echo "tbody tr:nth-child(even) {"
echo "  background: #f0f0f2;"
echo "}"
echo "td {"
echo "  border-bottom: 1px solid #cecfd5;"
echo "  border-right: 1px solid #cecfd5;"
echo "}"
echo "td:first-child {"
echo "  border-left: 1px solid #cecfd5;"
echo "}"
echo ".book-title {"
echo "  color: #395870;"
echo "  display: block;"
echo "}"

echo ".item-stock,"
echo ".item-qty {"
echo "  text-align: center;"
echo "}"
echo ".item-price {"
echo "  text-align: right;"
echo "}"
echo ".item-multiple {"
echo "  display: block;"
echo "}"
echo "tfoot {"
echo "  text-align: right;"
echo "}"
echo "tfoot tr:last-child {"
echo "  background: #f0f0f2;"
echo "}"
echo "</style>"


#######################################################
#######################################################
## CKF-Forecasting Workflow      	       	     ##
#######################################################
#######################################################
TODAY_DATE=$(date)
echo "Report Created at" ${TODAY_DATE}" <p>"

echo "<hr>"
echo "Workflow Name : CKF-Forecasting <p>"
echo "<hr>"

###---------------------------###
### Header                    ###
###---------------------------###


echo "HIVE_FCST_DB: " ${HIVE_FCST_DB} "<p>"

echo "<table>"

echo "<thead>"
echo "<tr>"
echo " <td>Number of items considered</td>"
echo "</tr>"
echo "</thead>"

echo "<tbody>"

# oozie
HIVE_HEADER="SET mapreduce.job.credentials.binary=${HADOOP_TOKEN_FILE_LOCATION}; SET tez.credentials.path=${HADOOP_TOKEN_FILE_LOCATION}; SET hive.execution.engine=${HIVE_ENGINE}; SET tez.queue.name=${QUEUE_NAME};"
HIVE_HEADER_MR="SET mapreduce.job.credentials.binary=${HADOOP_TOKEN_FILE_LOCATION}; SET tez.credentials.path=${HADOOP_TOKEN_FILE_LOCATION}; SET hive.execution.engine=mr; SET mapred.job.queue.name=${QUEUE_NAME};"

### --------------------------###
### Total Item counts              ###
###---------------------------###

ITEMS_FORECASTED_BY_CKF=`hive -e "${HIVE_HEADER} SELECT COUNT(DISTINCT mdse_item_i) FROM ${HIVE_FCST_DB}.${SCORE_OUTPUT_TABLE};"`
if [ $? -ne 0 ]
then
    echo "Hive error with ITEMS_FORECASTED_BY_CKF=" ${ITEMS_FORECASTED_BY_CKF}
fi


ITEMS_FORECASTED_COUNT=`hive -e " ${HIVE_HEADER}
SELECT COUNT(DISTINCT mdse_item_i)
FROM ${HIVE_FCST_DB}.${SCORE_OUTPUT_TABLE};"`
if [ $? -ne 0 ]
then
    echo "Hive error with ITEMS_FORECASTED_COUNT=" ${ITEMS_FORECASTED_COUNT}
fi

echo "<tr>"

echo  " <td>${ITEMS_FORECASTED_BY_CKF}</td>"
echo  " <td>${ITEMS_FORECASTED_COUNT}</td>"
echo  "</tr>"

### --------------------------###
### Null counts              ###
###---------------------------###


echo "<table>"

echo "<thead>"
echo "<tr>"
echo " <td>Count of Nulls - week end date</td>"
echo " <td>Count of Nulls - forecast_q</td>"
echo "</tr>"
echo "</thead>"

echo "<tbody>"

NULL_WEEK_COUNT=`hive -e " ${HIVE_HEADER}
SELECT COUNT(*)
FROM ${HIVE_FCST_DB}.${SCORE_OUTPUT_TABLE} WHERE week_end_date is NULL;"`
if [ $? -ne 0 ]
then
    echo "Hive error with NULL_WEEK_COUNT=" ${NULL_WEEK_COUNT}
fi

NULL_FORECAST_COUNT=`hive -e " ${HIVE_HEADER}
SELECT COUNT(*)
FROM ${HIVE_FCST_DB}.${SCORE_OUTPUT_TABLE} WHERE predicted is NULL;"`
if [ $? -ne 0 ]
then
    echo "Hive error with NULL_FORECAST_COUNT=" ${NULL_FORECAST_COUNT}
fi

echo "<tr>"
echo  " <td>${NULL_WEEK_COUNT}</td>"
echo  " <td>${NULL_FORECAST_COUNT}</td>"
echo  "</tr>"

### --------------------------###
### Negative counts           ###
###---------------------------###


echo "<table>"

echo "<thead>"
echo "<tr>"
echo " <td>Count of Negative - prediction</td>"
echo "</tr>"
echo "</thead>"

echo "<tbody>"

NEGATIVE_FORECAST_COUNT=`hive -e "
SELECT COUNT(*)
FROM ${HIVE_FCST_DB}.${SCORE_OUTPUT_TABLE} WHERE predicted < 0;"`
if [ $? -ne 0 ]
then
    echo "Hive error with NEGATIVE_FORECAST_COUNT" ${NEGATIVE_FORECAST_COUNT}
fi

echo "<tr>"
echo  " <td>${NEGATIVE_FORECAST_COUNT}</td>"
echo  "</tr>"
