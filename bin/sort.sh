#!/bin/bash

if [ $# -gt 2 ];
then
  echo 'USAGE: bin/sort.sh file max_memory_lines'
  exit 1
fi

base_dir=$(dirname $0)/..

for file in $base_dir/dist/*.jar;
do
  CLASSPATH=$CLASSPATH:$file
done

for file in $base_dir/lib/*.jar;
do
  CLASSPATH=$CLASSPATH:$file
done

java -agentlib:hprof=cpu=samples,depth=10 -Xmx2G -server -cp $CLASSPATH voldemort.store.readonly.StringSorter ${1} ${2}