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

# Name says it all. Like all the other scripts the --config parameter allow
# you to determine, which configuration to use. Log output goes into a logs
# directory on each node (will be auto created if it doesn't exist).


usage="Usage: voldemort-start-all.sh [--config <conf-dir>]"

bin=`dirname "$0"`
bin=`cd "$bin"; pwd`

.  "$bin/voldemort-config.sh"

if [ "$#" != "0" ] && [ "$#" != "2" ]
then
	echo $usage
	exit 1
fi

#check to see if the conf dir is given as an optional argument
if [ $# -eq 2 ]
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

export HOSTLIST="$VOL_CONF_DIR/config/nodes"

for node in `cat "$HOSTLIST"`; do
 ssh $VOL_SSH_OPTS $node "mkdir $VOL_HOME/logs 2>/dev/null; nohup $bin/voldemort-server.sh `cd $VOL_CONF_DIR && pwd` >$VOL_HOME/logs/voldemort-server.log 2>$VOL_HOME/logs/voldemort-server.err </dev/null& " &
done
wait

