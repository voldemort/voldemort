#!/bin/bash

#
# Copyright 2010 LinkedIn, Inc
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

base_dir=$(dirname $0)/../../../

for file in $base_dir/dist/*.jar;
do
HADOOP_CLASSPATH=$HADOOP_CLASSPATH:$file
done

for file in $base_dir/lib/*.jar;
do
HADOOP_CLASSPATH=$HADOOP_CLASSPATH:$file
done

HADOOP_CLASSPATH=$HADOOP_CLASSPATH:$base_dir/dist/resources
export HADOOP_CLASSPATH

$HADOOP_HOME/bin/hadoop voldemort.store.readwrite.mr.HadoopRWStoreJobRunner $@ 
