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

# Syncs the entire voldemort install directory over to the other nodes using rsync. The
# script is smart enough to exclude the BDB data directories (if they are in
# their standard locations) to avoid overwriting data on the nodes. Once
# everything is synced over it prepares node.ids for each node and ships them
# over. The script is also driven by the 'nodes' file.

usage="Usage: voldemort-sync-config.sh [--config <conf-dir>]"

bin=`dirname "$0"`
bin=`cd "$bin"; pwd`

.  "$bin/voldemort-config.sh"

#check to see if the conf dir is given as an optional argument
if [ $# -eq 2 ]
then
    if [ "--config" = "$1" ]
	  then
	      shift
	      confdir=$1
	      shift
	      VOL_CONF_DIR=$confdir
    fi
fi

export HOSTLIST="$VOL_CONF_DIR/config/nodes"

echo -n "Syncing config changes... "

# don't copy the local berkeley db files to all nodes
excludefile="/tmp/rsync-excludes$$"
echo "bdb" > $excludefile
cd `cd $VOL_HOME && pwd`
find config -name bdb -type d >> $excludefile
cd -

# Sync everything but BDB files over
for node in `cat "$HOSTLIST"`; do
	rsync $VOL_SSH_OPTS --exclude-from $excludefile --delete -ra `cd $VOL_HOME && pwd`/ $node:`cd $VOL_HOME && pwd` && echo -n "$node " &	
done
# Wait until config is all pushed out.
wait
rm $excludefile
echo "done!"

echo "Reassigning node IDs..."
nodeid=0
abs_conf_dir=`cd $VOL_CONF_DIR && pwd`
for node in `cat "$HOSTLIST"`; do
	ssh $node $bin/voldemort-node-id.sh --config $abs_conf_dir $nodeid &
	nodeid=$[$nodeid + 1]
done
wait
echo "Done syncing Voldemort home directory and reassigning node IDs"
exit 0
