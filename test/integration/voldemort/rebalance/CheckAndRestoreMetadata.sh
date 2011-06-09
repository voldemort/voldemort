#!/bin/bash

VLDMDIR=/Users/lgao/Projects/voldemort
WORKDIR=/Users/lgao/work/rebalance
TMPCLUSTER=/Users/lgao/work/rebalance/tmpcluster
host[0]="tcp://localhost:6667"
host[1]="tcp://localhost:6669"
host[2]="tcp://localhost:6671"

for srvs in {0..2}
do
  if [ "$srvs" -ne $1 ]
  then
    $VLDMDIR/bin/voldemort-admin-tool.sh --url ${host[$srvs]} --node $srvs --get-metadata cluster.xml --outdir $TMPCLUSTER > /dev/null 
    diff $WORKDIR/initial-cluster.xml $TMPCLUSTER/cluster.xml.$srvs > /dev/null
    if [ "$?" -ne "0" ]
    then
      echo Inconsistent metadata found - $TMPCLUSTER/cluster.xml.$srvs
      exit -1
    fi

    # restore the state after the state is validated
    $WORKDIR/RestoreMetadata.sh $srvs

  fi
done