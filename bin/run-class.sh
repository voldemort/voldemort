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

if [ $# -lt 1 ]; then
	echo $0 java-class-name [options]
	exit 1
fi

base_dir="$( cd "$( dirname "${BASH_SOURCE[0]}" )"/.. && pwd )"

for file in $base_dir/lib/*.jar;
do
	CLASSPATH=$CLASSPATH:$file
done

for file in $base_dir/contrib/*/lib/*.jar;
do
  CLASSPATH=$CLASSPATH:$file
done

for file in $base_dir/dist/*.jar;
do
  CLASSPATH=$CLASSPATH:$file
done
CLASSPATH=$CLASSPATH:$base_dir/dist/resources

if [ -z "$VOLD_OPTS" ]; then
  VOLD_OPTS="-Xmx2G -server -Dcom.sun.management.jmxremote "
fi

# add '-Dlog4j.debug ' to debug log4j issues if not specify log4j.properties
if [ -z ${LOG4JPROPERTIES} ]; then
  LOG4JPROPERTIES="-Dlog4j.configuration=file://${base_dir}/src/java/log4j.properties"
fi

export CLASSPATH
java $LOG4JPROPERTIES $VOLD_OPTS -cp $CLASSPATH $@
