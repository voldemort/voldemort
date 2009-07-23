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

# Stops the local node. Note: As a future enhancement the PID of the current
# running voldemort server could be saved, so finding out, which Java process
# to end would be more reliable in case there are multiple Voldemort
# instances running on the same host under the same user. The PID would be
# ideally saved on a per config basis.


usage="Usage: voldemort-stop-all.sh [--config <conf-dir>]"

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
 ssh $VOL_SSH_OPTS $node "$bin/voldemort-stop.sh" &
done
wait

