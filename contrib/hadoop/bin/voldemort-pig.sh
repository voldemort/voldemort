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

base_dir=$(dirname $0)/../../..

CLASSPATH=$home_dir/common/hadoop-conf/dev/conf:$home_dir/dist/jobs/

for file in $base_dir/dist/*.jar;
do
  CLASSPATH=$CLASSPATH:$file
done

for file in $base_dir/lib/*.jar;
do
  CLASSPATH=$CLASSPATH:$file
done

for file in $base_dir/contrib/hadoop/lib/*.jar;
do
  CLASSPATH=$CLASSPATH:$file
done

if [ "x$PIG_HOME" = "x" ]; then
    echo 'Please set PIG_HOME'
    exit 1
fi

JAR=$PIG_HOME/pig.jar
if [ ! -e $JAR ]; then
    echo 'Could not find Pig jar' 
    exit 1
fi
CLASSPATH=$CLASSPATH:$JAR

export CLASSPATH 

java $PIG_JAVA_OPTS -Dudf.import.list=voldemort.hadoop.pig -cp $CLASSPATH org.apache.pig.Main $*
