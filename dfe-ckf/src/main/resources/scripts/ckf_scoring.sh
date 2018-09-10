#!/bin/bash

set -xv
set -o pipefail

#empty trash
#hadoop fs -expunge

export SCORE_INPUT_DIR=$1
export SCORE_OUTPUT_DIR=$2
export queue_name=$3
export LATEST_MODEL_PATH=$4/ckf_models

# clear model landing zone keep it until new one comes in
hadoop fs -rm -f -r -skipTrash ${SCORE_OUTPUT_DIR}
#hadoop fs -mkdir ${SCORE_OUTPUT_DIR}

PYTHON_DIR=/usr/local/python-tgt/bin

# run streaming model training in R
hadoop jar /usr/hdp/current/hadoop-mapreduce-client/hadoop-streaming.jar \
-Dmapreduce.job.queuename=${queue_name} \
-Dmapreduce.task.timeout=0 \
-file ckf_mapper_scoring.py \
-file ckf_reducer_scoring.py \
-file algo_integration_scoring.py \
-file ckf_np.py \
-file config.csv \
-mapper "${PYTHON_DIR}/python2.7 ckf_mapper_scoring.py" \
-reducer "${PYTHON_DIR}/python2.7 ckf_reducer_scoring.py ${LATEST_MODEL_PATH}" \
-input ${SCORE_INPUT_DIR} \
-output ${SCORE_OUTPUT_DIR} \
-numReduceTasks 1100


