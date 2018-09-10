#!/bin/bash

set -xv
set -o pipefail

MODEL_PATH=$1
TEMP_DIR="temp_dir"
TEMP_OUT="temp_out"

mkdir $TEMP_DIR
mkdir $TEMP_OUT

hadoop fs -copyToLocal $MODEL_PATH/* $TEMP_DIR

python shuffleuid_models.py $TEMP_DIR $MODEL_PATH $TEMP_OUT

rm -rf $TEMP_DIR

hadoop fs -rm -skipTrash $MODEL_PATH/*.txt
hadoop fs -put $TEMP_OUT/* $MODEL_PATH
rm -rf $TEMP_OUT
