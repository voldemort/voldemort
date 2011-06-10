#!/bin/bash

source setup_env.inc
LOGFILE=$1
LOGDIR=$WORKDIR/log
ENDMETADATA=end-cluster.xml

cd $VLDMDIR
bin/voldemort-rebalance.sh --no-delete --current-cluster ${TESTCFG_PREFIX}1/config/cluster.xml --current-stores ${TESTCFG_PREFIX}1/config/stores.xml --target-cluster $WORKDIR/$ENDMETADATA --output-dir $WORKDIR > $LOGDIR/$LOGFILE 2>&1 &

# make sure rebalancing starts
cd $WORKDIR
$WORKDIR/WaitforOutput.sh "Node 0 (localhost) is ready for rebalance" $LOGDIR/$LOGFILE
$WORKDIR/WaitforOutput.sh "Node 1 (localhost) is ready for rebalance" $LOGDIR/$LOGFILE
$WORKDIR/WaitforOutput.sh "Node 2 (localhost) is ready for rebalance" $LOGDIR/$LOGFILE
