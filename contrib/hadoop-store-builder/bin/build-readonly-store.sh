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

base_dir=$(dirname $0)/../../../

if [ -z "$VOLDEMORT_CONFIG_DIR" ]; then
  VOLDEMORT_CONFIG_DIR=$base_dir/config
fi

source $VOLDEMORT_CONFIG_DIR/voldemort-env.sh

for file in $base_dir/dist/*.jar;
do
  CLASSPATH=$CLASSPATH:$file
done

for file in $base_dir/lib/*.jar;
do
  CLASSPATH=$CLASSPATH:$file
done

CLASSPATH=$CLASSPATH:$base_dir/dist/resources

if [ -z "$VOLD_OPTS" ]; then
  VOLD_OPTS="-Xmx2G -server"
fi

echo "Using JAVA_HOME = $JAVA_HOME"
exec $JAVA_HOME/bin/java -Dlog4j.configuration=file:$VOLDEMORT_CONFIG_DIR/log4j.properties $VOLD_OPTS -cp $CLASSPATH voldemort.store.readonly.JsonStoreBuilder $@
