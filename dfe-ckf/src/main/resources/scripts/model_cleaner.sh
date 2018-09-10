#!/bin/bash

set -xv
set -o pipefail

#COMMON_DIR=$1
#INPUT=$COMMON_DIR/ckf_chain_model_obj
INPUT="$1"
echo $INPUT

thirdLastModelDir=''
secondLastModelDir=''
lastModelDir=''

for dir in $(hadoop fs -ls $INPUT/ | grep -o -e "$INPUT/.*") ; do
  thirdLastModelDir=$secondLastModelDir
  secondLastModelDir=$lastModelDir
  lastModelDir=$dir
done


echo "result:"
for dir in $(hadoop fs -ls $INPUT/ | grep -o -e "$INPUT/.*") ; do
  if [ "$dir" != "$secondLastModelDir" ] && [ "$dir" != "$lastModelDir" ] && [ "$dir" != "$thirdLastModelDir" ];  then
    hadoop fs -rm -r -skipTrash $dir
  fi
done
