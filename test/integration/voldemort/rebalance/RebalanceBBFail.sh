#!/bin/bash

source setup_env.inc
ENDMETADATA=end-cluster.xml
TERMSTRING="Successfully terminated rebalance all tasks"
CLUSTERGENLOG=cluster_gen.log

# restore the config
echo restore config on all nodes...
let i=0 
while [ $TOTAL_NUM_SERVERS -gt $i ]
do
  let SERVERID=$i
  let i+=1;
  if [ "$REMOTE_RUN" == "true" ]; then
    export remote_call="ssh $REMOTEUSER@${SERVER_MACHINES[$SERVERID]}"
  else
    export remote_call=eval
  fi

  $remote_call "cd $REMOTEWORK; source setup_env.inc; cd \$WORKDIR; rm -rf \$LOGDIR; rm -rf \$TMPCLUSTER; mkdir \$LOGDIR; mkdir \$TMPCLUSTER"
  $remote_call "cd $REMOTEWORK; source setup_env.inc; cd \$WORKDIR; rm -rf \${TESTCFG_PREFIX}$i/config/*; mkdir -p \${TESTCFG_PREFIX}$i; cp -rf $CONFIGSOURCE$i/config \${TESTCFG_PREFIX}$i/"
  if [ "$1" == "reload" -o "$1" == "copy" ]
  then
    $remote_call "cd $REMOTEWORK; source setup_env.inc; cd \$WORKDIR; rm -rf \${TESTCFG_PREFIX}$i/data/bdb/*"
  fi
done

# restore servers to their initial state
#echo Restore server initial state
#$WORKDIR/RestoreServers.sh

# generate the target cluster.xml
echo Generate target cluster.xml
cd $VLDMDIR
bin/voldemort-rebalance.sh --current-cluster $WORKDIR/less-nodes-cluster.xml --current-stores $WORKDIR/stores.xml --target-cluster $WORKDIR/more-nodes-cluster.xml --generate --output-dir $WORKDIR > $LOGDIR/$CLUSTERGENLOG
cd $WORKDIR

# save the end-cluster.xml
cp $WORKDIR/final-cluster.xml $WORKDIR/$ENDMETADATA

if [ "$1" == "copy" ]
then
  let i=0
  while [ $TOTAL_OLD_SERVERS -gt $i ]
  do 
    let SERVERID=$i
    let i+=1;
    if [ "$REMOTE_RUN" == "true" ]; then
      export remote_call="ssh $REMOTEUSER@${SERVER_MACHINES[$SERVERID]}"
    else
      export remote_call=eval
    fi
    echo Copying data ...
    $remote_call "cd $REMOTEWORK; source setup_env.inc; mkdir -p \${TESTCFG_PREFIX}$i/data/bdb; cp $DATASOURCE$i/data/bdb/* \${TESTCFG_PREFIX}$i/data/bdb/"
  done
fi

# start all servers (including the new one)
echo Start all servers ...
$WORKDIR/BootstrapAll.sh all

if [ "$1" == "reload" ]
then
  # populate workload
  echo Populating initial workload...
  $WORKDIR/initWorkloadGen.sh $2 $3
fi

# restore keys for validation check if required
echo Restoring keys for validation ...
RestoreKeys.sh

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

