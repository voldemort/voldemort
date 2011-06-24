#!/bin/bash

set -x
source setup_env.inc

SERVSTRING="VoldemortServer $TESTCFG_PREFIX"

let i=0
while [ ${TOTAL_NUM_SERVERS} -gt $i ] 
do
    export remote_call="ssh $REMOTEUSER@${SERVER_MACHINES[$i]}"
    let i+=1
    $remote_call "cd $REMOTEWORK; source setup_env.inc; kill \`ps -ef | grep \"VoldemortServer \${TESTCFG_PREFIX}$i\" | grep -v grep | awk '{print \$2}'\`"
done