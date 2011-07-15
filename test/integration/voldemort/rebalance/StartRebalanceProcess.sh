#!/bin/bash

source setup_env.inc
LOGFILE=$1
LOGDIR=$WORKDIR/log
ENDMETADATA=end-cluster.xml

cd $VLDMDIR
bin/voldemort-rebalance.sh --current-cluster $WORKDIR/initial-cluster.xml --current-stores $WORKDIR/stores.xml --target-cluster $WORKDIR/$ENDMETADATA --parallelism 2 --output-dir $WORKDIR > $LOGDIR/$LOGFILE 2>&1 &

echo Checking for nodes rebalancing readniess... 
# make sure rebalancing starts
let i=0
while [ $TOTAL_NUM_SERVERS -gt $i ]
do
#  MSG="Node "$i" ("${SERVER_MACHINES[$i]}") is ready"
  MSG="Node "$i""
bash -x  $WORKDIR/WaitforOutput.sh "$MSG" $LOGDIR/$LOGFILE
  let i+=1
done
