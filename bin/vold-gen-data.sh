#!/bin/bash

# Check argument
[ $# -lt 1 ] && echo "Arguments : #keys <output_file>" && exit -1;

NUM_KEYS=${1};
OUTPUT_FILE="bin/vold_auto_populate.txt"

[ $# -gt 1 ] && OUTPUT_FILE=${2};

# Delete output file if it already exists
`rm -f ${OUTPUT_FILE} > /dev/null 2>&1`

for((i=0;i<${NUM_KEYS};i++))
do
  echo "put \"${i}\" \"${i}_value\"" >> ${OUTPUT_FILE}
done

