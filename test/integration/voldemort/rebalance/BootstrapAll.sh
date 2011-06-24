#!/bin/bash

source setup_env.inc

let ALLSERVERS=$TOTAL_OLD_SERVERS
if [ "$1" = "all" ]
then 
  let ALLSERVERS=$TOTAL_NUM_SERVERS
fi

let i=0 
while [ $ALLSERVERS -gt $i ]
do
  $WORKDIR/StartServer.sh $i
  $WORKDIR/RestoreMetadata.sh $i
  let i+=1
done

