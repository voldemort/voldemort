#!/bin/bash

VLDMDIR=/Users/lgao/Projects/voldemort
WORKDIR=/Users/lgao/work/rebalance
LOGDIR=$WORKDIR/log
ENDMETADATA=end-cluster.xml
TERMSTRING="Successfully terminated rebalance all tasks"
CLUSTERGENLOG=cluster_gen.log

# restore servers to their initial state
echo Restore server initial state
RestoreServers.sh

# generate the target cluster.xml
echo Generate target cluster.xml
$VLDMDIR/bin/voldemort-rebalance.sh --current-cluster $VLDMDIR/config/test_config1/config/cluster.xml --current-stores $VLDMDIR/config/test_config1/config/stores.xml --target-cluster $VLDMDIR/config/test_config3/config/cluster.xml --generate --output-dir $WORKDIR > $LOGDIR/$CLUSTERGENLOG

# save the end-cluster.xml
cp $WORKDIR/final-cluster.xml $WORKDIR/$ENDMETADATA

# start all servers (including the new one)
echo Start all servers
$WORKDIR/BootstrapAll.sh all

echo starting rebalance
LOGFILE=rebalance.log.`date +%H%M%S`
$WORKDIR/StartRebalanceProcess.sh $LOGFILE
grep "${TERMSTRING}" $LOGDIR/$LOGFILE > /dev/null 2>&1
while [ 1 ]
do
  # Randomly kill servers after some random time period 
  # and make sure rollback is successful and all metadata are as expected
  # If rebalance finishes, exit
  $WORKDIR/RandomWaitKillAndVerify.sh $LOGFILE

  # restart rebalance
  echo restarting rebalance
  LOGFILE=rebalance.log.`date +%H%M%S`
  $WORKDIR/StartRebalanceProcess.sh $LOGFILE
done