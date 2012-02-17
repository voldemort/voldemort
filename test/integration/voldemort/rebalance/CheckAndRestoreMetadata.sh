#!/bin/bash

source setup_env.inc
TCPPREFIX=tcp://

let srvs=0
while [ $TOTAL_NUM_SERVERS -gt $srvs ]
do
  if [ "$srvs" -ne $1 ]
  then
    $VLDMDIR/bin/voldemort-admin-tool.sh --url $TCPPREFIX${SERVER_MACHINES[$srvs]}:${SERVER_PORT[$srvs]} --node $srvs --get-metadata cluster.xml --outdir $TMPCLUSTER > /dev/null 
    diff $METADIR/initial-cluster.xml $TMPCLUSTER/cluster.xml_$srvs > /dev/null
    if [ "$?" -ne "0" ]
    then
      echo Inconsistent metadata found - $TMPCLUSTER/cluster.xml.$srvs
      exit -1
    fi

    # restore the state after the state is validated
    $WORKDIR/RestoreMetadata.sh $srvs
  fi
  let srvs+=1
done
