#!/bin/bash

#
#   Copyright 2008-2009 bebo, Inc
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

# Script to remove and change the node.id from local server.properties. Is
# also used by the sync script across all nodes.

usage="Usage: voldemort-node-id.sh [--config <conf-dir>] <node id>"

bin=`dirname "$0"`
bin=`cd "$bin"; pwd`

.  "$bin/voldemort-config.sh"

if [ $# -eq 0 ] || [ $# -eq 2 ] || [ $# -gt 3 ]
then
	echo $usage
	exit 1
fi

nodeid=$1

#check to see if the conf dir is given as an optional argument
if [ $# -eq 3 ]
then
    if [ "--config" = "$1" ]
	  then
	      shift
	      confdir=$1
	      shift
	      VOL_CONF_DIR=$confdir
	      nodeid=$1
    fi
fi

SERVER_PROPERTIES="$VOL_CONF_DIR/config/server.properties"

grep -v "node.id" $SERVER_PROPERTIES > /tmp/$$
echo "node.id=$nodeid" >> /tmp/$$
cp -f /tmp/$$ $SERVER_PROPERTIES
rm /tmp/$$

echo $(hostname): node.id=$nodeid
