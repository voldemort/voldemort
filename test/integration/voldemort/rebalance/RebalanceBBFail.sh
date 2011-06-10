#!/bin/bash

source setup_env.inc
LOGDIR=$WORKDIR/log
DATADIR=$WORKDIR/data
ENDMETADATA=end-cluster.xml
TERMSTRING="Successfully terminated rebalance all tasks"
CLUSTERGENLOG=cluster_gen.log

rm -rf $LOGDIR
rm -rf $TMPCLUSTER
mkdir $LOGDIR
mkdir $TMPCLUSTER

# restore the config
rm -rf $TESTCFG_DIR
cp -rf $ORIGCFG_DIR $TESTCFG_DIR
for i in 1 2 3; do mkdir -p $TESTCFG_PREFIX$i/config; done

if [ $1 = "reload" ]
then
  for i in 1 2 3; do rm -rf $TESTCFG_PREFIX$i/data/*; done
fi

# restore servers to their initial state
echo Restore server initial state
$WORKDIR/RestoreServers.sh

# generate the target cluster.xml
echo Generate target cluster.xml
cd $VLDMDIR
bin/voldemort-rebalance.sh --current-cluster ${TESTCFG_PREFIX}1/config/cluster.xml --current-stores ${TESTCFG_PREFIX}1/config/stores.xml --target-cluster ${TESTCFG_PREFIX}3/config/cluster.xml --generate --output-dir $WORKDIR > $LOGDIR/$CLUSTERGENLOG
cd $WORKDIR

# save the end-cluster.xml
cp $WORKDIR/final-cluster.xml $WORKDIR/$ENDMETADATA

# start all servers (including the new one)
echo Start all servers
$WORKDIR/BootstrapAll.sh all

if [ $1 == "reload" ]
then
  # populate workload
  $WORKDIR/initWorkloadGen.sh $2 $3
fi

# restore keys for validation check if required
bash -x RestoreKeys.sh

# read -p "Press any key to continue..."

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
  let EXITCODE="$?"
  if [ "$EXITCODE" -eq "9" ]
  then
    # all done, exit
    exit 0
  fi

  # restart rebalance
  echo restarting rebalance
  LOGFILE=rebalance.log.`date +%H%M%S`
  $WORKDIR/StartRebalanceProcess.sh $LOGFILE
done

