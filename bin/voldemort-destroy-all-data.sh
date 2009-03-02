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

# Kills all the BDB data files after showing affected nodes and the
# respective data directory path. User must enter 'Yes' before it proceeds.


usage="Usage: voldemort-destroy-all-data.sh [--config <conf-dir>]"

bin=`dirname "$0"`
bin=`cd "$bin"; pwd`

.  "$bin/voldemort-config.sh"

if [ "$#" != "2" ] && [ "$#" != "0" ]
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

HOSTLIST="$VOL_CONF_DIR/config/nodes"
datadir="$(cd $VOL_CONF_DIR && pwd)/data/bdb"

# Make user confirm the selection
echo ------------------------------------------------------------------
echo ------------------------------------------------------------------
echo
echo You are about to delete all data on the following nodes:
echo 
echo $(cat $HOSTLIST)
echo 
echo in $datadir
echo
echo 
echo ------------------------------------------------------------------
echo ------------------------------------------------------------------

echo -n "Type 'Yes' if you would like to continue: "
read a
if [[ $a != "Yes" ]]
then
        echo "Didn't get a 'Yes' input. Quitting for now."
        exit 1
fi


for node in `cat "$HOSTLIST"`; do
 echo $node: Removing data directory $datadir
 ssh $VOL_SSH_OPTS $node "rm -rf $datadir" &
done
wait
echo
echo "Done destroying the data directories."
