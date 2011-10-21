#!/bin/bash

source setup_env.inc
TERMSTRING="Successfully terminated rebalance all tasks"
CLUSTERGENLOG=cluster_gen.log

$WORKDIR/KillAllServers.sh 

rm -rf $LOGDIR
rm -rf $TMPCLUSTER 
mkdir $LOGDIR 
mkdir $TMPCLUSTER

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
  $remote_call "cd $REMOTEWORK; source setup_env.inc; cd \$WORKDIR; cp \$METADIR/initial-cluster.xml \${TESTCFG_PREFIX}$i/config/cluster.xml; cp \$METADIR/stores.xml \${TESTCFG_PREFIX}$i/config/stores.xml"
  if [ "$1" == "reload" -o "$1" == "copy" ]
  then
    $remote_call "cd $REMOTEWORK; source setup_env.inc; cd \$WORKDIR; rm -rf \${TESTCFG_PREFIX}$i/data/bdb/*"
  fi
done

# generate the target cluster.xml
if [ $GENERATE_CLUSTER == "TRUE" ]; then
  echo Generate target cluster.xml
  cd $VLDMDIR
  bin/voldemort-rebalance.sh --current-cluster $METADIR/target-cluster.xml --current-stores $METADIR/stores.xml --target-cluster $METADIR/initial-cluster.xml --generate --output-dir $METADIR > $LOGDIR/$CLUSTERGENLOG
  cd $WORKDIR
fi

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
if [ "$1" == "reload" -a "$4" == "entropy" -o "$1" == "copy" -a "$2" == "entropy" ]
then
  $WORKDIR/RestoreKeys.sh
fi

echo starting rebalance
LOGFILE=rebalance.log.`date +%H%M%S`
$WORKDIR/StartRebalanceProcess.sh $LOGFILE
grep "${TERMSTRING}" $LOGDIR/$LOGFILE > /dev/null 2>&1

ERROR_MSG="ERROR Unsuccessfully terminated rebalance operation"
TERMSTRING="Successfully terminated rebalance all tasks"
if [ "$KILLMODE" == "NONE" ]; then
  echo waiting for rebalancing process to terminate...
  $WORKDIR/WaitforOutput.sh "$ERROR_MSG" "$TERMSTRING" $LOGDIR/$LOGFILE
  exit 0
fi

if [ "$KILLMODE" == "STEALER-ONLY" ]; then
  while [ 1 ]; do
    $WORKDIR/RandomKillStealer.sh $LOGFILE
    let EXITCODE="$?"
    if [ "$EXITCODE" -eq "9" ]; then
      # all done, exit
      $WORKDIR/ValidateData.sh
      let EXITCODE="$?"
      if [ "$EXITCODE" -ne "0" ]; then
	  echo "Data validation failed! Check $LOGDIR/$LOGFILE for details!"
	  exit $EXITCODE
      else
          echo "Rebalancing finished successfully !!!"
          exit 0
      fi
    fi
  done
else
  while [ 1 ]; do
    # Randomly kill servers after some random time period 
    # and make sure rollback is successful and all metadata are as expected
    # If rebalance finishes, exit
    $WORKDIR/RandomWaitKillAndVerify.sh $LOGFILE
    let EXITCODE="$?"
    if [ "$EXITCODE" -eq "9" ]; then
      # all done, exit
      echo "Rebalancing finished successfully !!!"
      exit 0
    fi

    # restart rebalance
    echo restarting rebalance
    LOGFILE=rebalance.log.`date +%H%M%S`
    $WORKDIR/StartRebalanceProcess.sh $LOGFILE
  done
fi
