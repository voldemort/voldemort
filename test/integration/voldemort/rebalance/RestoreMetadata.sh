#!/bin/bash                                                                                  

source setup_env.inc
HOSTPREFIX="tcp://"

cd $VLDMDIR
let srvs=0
while [ $TOTAL_NUM_SERVERS -gt $srvs ]
do
  if [ "$srvs" -eq $1 ]
  then
    bin/voldemort-admin-tool.sh --url $HOSTPREFIX${SERVER_MACHINES[$srvs]}:${SERVER_PORT[$srvs]} --node $srvs --clear-rebalancing-metadata > /dev/null
  fi
  let srvs+=1
done
