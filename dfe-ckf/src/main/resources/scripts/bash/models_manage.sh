#!/bin/bash
set -xv

MODELS_OP_BASE_DIR=$1
MODELS_TO_KEEP=$2

# Retrieving Current Models Count.
# Substracing 1 due to having extra info as one line "Found X items" while listing to get exact count.
MODELS_AVL=`expr $(hadoop fs -ls $MODELS_OP_BASE_DIR | wc -l) - 1`
MODELS_TO_DEL=`expr $MODELS_AVL - $MODELS_TO_KEEP`

#Listing the Model Directories
hadoop fs -ls -t $MODELS_OP_BASE_DIR

#Deleting Models if Models Available is greater than Models to keep(Parameter).
#If $MODELS_TO_KEEP is -1 then Models will not be Deleted.
if [[ $MODELS_AVL -gt $MODELS_TO_KEEP && $MODELS_TO_KEEP -ne -1 ]]; then

    for dir in $(hadoop fs -ls -t $MODELS_OP_BASE_DIR | awk '{ print $8; }' | tail -$MODELS_TO_DEL) ; do

        hadoop fs -rm -r -skipTrash $dir
    done

else
    echo "Either Models are less than or equal to the list to keep OR Instructed NOT to DELETE"
    echo "MODELS_AVL: $MODELS_AVL"
    echo "MODELS_TO_KEEP: $MODELS_TO_KEEP"
fi
