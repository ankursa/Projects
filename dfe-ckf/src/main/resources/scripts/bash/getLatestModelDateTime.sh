#!/bin/bash

export MODELS_OP_BASE_DIR=$1

# Retrieve latest model directory for scoring
LATEST_MODEL_DATETIME=`hadoop fs -ls -t $MODELS_OP_BASE_DIR | awk -F/ 'FNR==2{ print $NF }'`

echo LATEST_MODEL_DATETIME=${LATEST_MODEL_DATETIME}

