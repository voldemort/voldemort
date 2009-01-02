#!/bin/bash

base_dir=$(dirname $0)/..

for file in $base_dir/lib/*.jar;
do
	CLASSPATH=$CLASSPATH:$file
done

for file in $base_dir/dist/*.jar;
do
  CLASSPATH=$CLASSPATH:$file
done

export CLASSPATH
java -Xmx2G -server -cp $CLASSPATH ${1} ${2} ${3} ${4} ${5} ${6} ${7} 