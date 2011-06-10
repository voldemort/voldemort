#!/bin/bash                                                                                  

source setup_env.inc
HOST[0]="tcp://localhost:6667"
HOST[1]="tcp://localhost:6669"
HOST[2]="tcp://localhost:6671"

for srvs in {0..2}
do
  if [ "$srvs" -eq $1 ]
  then
    $VLDMDIR/bin/voldemort-admin-tool.sh --url ${HOST[$srvs]} --node $srvs --clear-rebalancing-metadata > /dev/null
  fi
done
