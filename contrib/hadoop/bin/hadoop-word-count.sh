#!/bin/bash

#
#   Copyright 2010 LinkedIn, Inc
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#   you may not use this file except in compliance with the License.
#   You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
#

if [ $# -ne 2 ];
then
	echo 'USAGE: bin/hadoop-word-count.sh store_name admin_bootstrap_url'
	exit 1
fi

base_dir=$(dirname $0)/../../..

for file in $base_dir/dist/*.jar;
do
  HADOOP_CLASSPATH=$HADOOP_CLASSPATH:$file
done

for file in $base_dir/lib/*.jar;
do
  HADOOP_CLASSPATH=$HADOOP_CLASSPATH:$file
done

if [ -n "${HADOOP_HOME}" ]; 
then
	HADOOP_CLASSPATH=$HADOOP_CLASSPATH:$HADOOP_HOME	
else
	echo 'Missing HADOOP_HOME'
	exit 1
fi

HADOOP_CLASSPATH=$HADOOP_CLASSPATH:$base_dir/dist/resources:$VOLDEMORT_HOME/lib/
export HADOOP_CLASSPATH

$HADOOP_HOME/bin/hadoop voldemort.hadoop.VoldemortWordCount $@ 
