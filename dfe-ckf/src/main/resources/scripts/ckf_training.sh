#!/bin/bash

set -xv
set -o pipefail

#empty trash
#hadoop fs -expunge

export TRAIN_INPUT_DIR=$1
export queue_name=$2
export MODEL_PATH=$3
export DATA_DIR=$4
#export MAX_WKND_D=$5

# clear model landing zone keep it until new one comes in
hadoop fs -rm -f -r -skipTrash ${MODEL_PATH}
hadoop fs -mkdir ${MODEL_PATH}
hadoop fs -rm -r -skipTrash ${DATA_DIR}/noout

PYTHON_DIR=/usr/local/python-tgt/bin

# run streaming model training in R
hadoop jar /usr/hdp/current/hadoop-mapreduce-client/hadoop-streaming.jar \
-Dmapreduce.job.queuename=${queue_name} \
-Dmapreduce.task.timeout=0 \
-file ckf_mapper_training.py \
-file ckf_reducer_training.py \
-file algo_integration_training.py \
-file ckf_np.py \
-file config.csv \
-mapper "${PYTHON_DIR}/python2.7 ckf_mapper_training.py" \
-reducer "${PYTHON_DIR}/python2.7 ckf_reducer_training.py ${MODEL_PATH}" \
-input ${TRAIN_INPUT_DIR} \
-output ${DATA_DIR}/noout \
-numReduceTasks 2200


