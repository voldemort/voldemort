#!/bin/bash

EXPECTED_ARGS=2

if [ $# -ne $EXPECTED_ARGS ]
then
  echo "Usage:  $0 [NUM_OF_KEYS] [MAX_VALUE_SIZE]"
  exit -1 
fi

KEYS=$1
RANGE=$2
i=0
while [[ $i -le $1 ]]
do
  echo -n put \"$i\" " \""
  number=$RANDOM
  let "number %= $RANGE"
  let "number += 1"
  cat /dev/random | uuencode -m -| head -n 2 | tail -1 | cut -b 1-$number 
  i=$((i + 1)) 
done

