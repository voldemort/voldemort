#!/bin/bash

#
#   Copyright 2008-2013 LinkedIn, Inc
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

# Start up a Voldemort REST Coordinator speaking to a Voldemort Cluster. 
# Example : 
# 1. Bring up a Voldemort single node cluster 
#     $ bin/voldemort-server.sh config/single_node_cluster
# 2. Bring up a Voldemort Coordinator
#     $ bin/voldemort-coordinator.sh config/coordinator_sample_config/config.properties
# 3. Do operations
#     See config/coordinator_sample_config/curl_samples.txt
if [ $# -gt 1 ];
then
	echo 'USAGE: bin/voldemort-coordinator.sh <coordinator-config-file>'
	exit 1
fi

base_dir=$(dirname $0)/..

for file in $base_dir/dist/*.jar;
do
  CLASSPATH=$CLASSPATH:$file
done

for file in $base_dir/public-lib/*.jar;
do
  CLASSPATH=$CLASSPATH:$file
done

for file in $base_dir/private-lib/*.jar;
do
  CLASSPATH=$CLASSPATH:$file
done

for file in $base_dir/contrib/*/lib/*.jar;
do
  CLASSPATH=$CLASSPATH:$file
done

CLASSPATH=$CLASSPATH:$base_dir/dist/resources

if [ -z "$VOLD_OPTS" ]; then
  VOLD_OPTS="-Xmx1G -server -Dcom.sun.management.jmxremote"
fi

java -Dlog4j.configuration=src/java/log4j.properties $VOLD_OPTS -cp $CLASSPATH voldemort.rest.coordinator.CoordinatorService $@
