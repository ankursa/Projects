#!/bin/bash

set -vx

export QUEUE_NAME=$1
export TRAIN_DB=$2
export TRAIN_INPUT_TABLE=$3

# EEFE-2896 changed execution engine to Tez
MAX_WKND_D=$(hive -e "SET hive.execution.engine=tez;
SET mapreduce.job.queuename=${QUEUE_NAME};
SET mapreduce.job.credentials.binary=${HADOOP_TOKEN_FILE_LOCATION};
select max(week_end_date)
from ${TRAIN_DB}.${TRAIN_INPUT_TABLE}")

echo "MAX_WKND_D=${MAX_WKND_D}"
