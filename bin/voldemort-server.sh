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

if [ $# -gt 2 ];
then
	echo 'USAGE: bin/voldemort-server.sh [voldemort_home] [voldemort_config_dir]'
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

for file in $base_dir/contrib/*/lib/*.jar;
do
  CLASSPATH=$CLASSPATH:$file
done

CLASSPATH=$CLASSPATH:$base_dir/dist/resources

if [ -z "$VOLD_OPTS" ]; then
  VOLD_OPTS="-Xmx2G -server -Dcom.sun.management.jmxremote"
fi

# add '-Dlog4j.debug ' to debug log4j issues.
LOG4JPROPERTIES="-Dlog4j.configuration=file:///${base_dir}/src/java/log4j.properties"

java $LOG4JPROPERTIES $VOLD_OPTS -cp $CLASSPATH voldemort.server.VoldemortServer $@
