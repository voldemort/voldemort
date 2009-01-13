#!/bin/bash

if [ $# -gt 1 ];
then
	echo 'USAGE: bin/voldemort-server.sh [voldemort_home]'
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

echo classpath=$CLASSPATH
java -Xmx2G -server -cp $CLASSPATH -Dcom.sun.management.jmxremote voldemort.server.VoldemortServer ${1}
