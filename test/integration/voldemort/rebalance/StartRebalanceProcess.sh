#!/bin/bash

LOGFILE=$1
VLDMDIR=/Users/lgao/Projects/voldemort
WORKDIR=/Users/lgao/work/rebalance
LOGDIR=$WORKDIR/log
ENDMETADATA=end-cluster.xml

cd $VLDMDIR
bin/voldemort-rebalance.sh --current-cluster $VLDMDIR/config/test_config1/config/cluster.xml --current-stores $VLDMDIR/config/test_config1/config/stores.xml --target-cluster $WORKDIR/$ENDMETADATA --output-dir $WORKDIR > $LOGDIR/$LOGFILE 2>&1 &

# make sure rebalancing starts                                                                      
$WORKDIR/WaitforOutput.sh "Node 0 (localhost) is ready for rebalance" $LOGDIR/$LOGFILE
$WORKDIR/WaitforOutput.sh "Node 1 (localhost) is ready for rebalance" $LOGDIR/$LOGFILE
$WORKDIR/WaitforOutput.sh "Node 2 (localhost) is ready for rebalance" $LOGDIR/$LOGFILE
