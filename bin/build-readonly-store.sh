#!/bin/bash

#
#   Copyright 2008-2009 LinkedIn, Inc
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

if [ $# != 7 ];
then
	echo 'USAGE: bin/build-readonly-store.sh cluster.xml store_definitions.xml store_name sort_obj_buffer_size input_data output_dir num_threads'
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

CLASSPATH=$CLASSPATH:$base_dir/dist/resources

java -Xmx2G -server -cp $CLASSPATH voldemort.store.readonly.JsonStoreBuilder ${1} ${2} ${3} ${4} ${5} ${6} ${7}