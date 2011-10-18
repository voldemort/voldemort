#!/bin/bash

set -x
source setup_env.inc

LOGDIR=$WORKDIR/log
LOGFILE=$1
TERMSTRING="Successfully terminated rebalance all tasks"

# grep for completion string in the output   
grep "${TERMSTRING}" $LOGDIR/$LOGFILE > /dev/null 2>&1
if [ "$?" -eq 0 ]
then
  echo rebalancing finished!!!!!!
  exit 9 
fi

let STEALER_NUM=$TOTAL_NUM_SERVERS-$TOTAL_OLD_SERVERS
kill_or_suspend_array=(`python -c "import random; print ' '.join([random.choice(['kill','suspend']) for i in range(${NUMS_TO_KILL_OR_SUSPEND})])"`)
to_kill_pids=()

let BASEIDX=$RANDOM%$STEALER_NUM

for ((i=0; i < ${NUMS_TO_KILL_OR_SUSPEND} ; i++)); do
  let idx=($BASEIDX+$i)%$STEALER_NUM
  tokill=$TOTAL_OLD_SERVERS+$idx
  let runid=$tokill+1
  if [ "$REMOTE_RUN" == "true" ]; then
    export remote_call="ssh $REMOTEUSER@${SERVER_MACHINES[$tokill]}"
  else
    export remote_call=eval
  fi
  $remote_call "cd $REMOTEWORK; source setup_env.inc; kill \`ps -ef | grep \"VoldemortServer \${TESTCFG_PREFIX}${runid}\" | grep -v grep | awk '{print \$2}'\`"
done

# sleep for 100-1000 seconds
SLEEP_RANGE=900
let "stime=($RANDOM%$SLEEP_RANGE) + 100"
echo sleeping for $stime seconds...
sleep $stime

for ((i=0; i < ${NUMS_TO_KILL_OR_SUSPEND} ; i++)); do
  let idx=($BASEIDX+$i)%$STEALER_NUM
  killed=$TOTAL_OLD_SERVERS+$idx
  $WORKDIR/StartServer.sh $killed
done

# sleep for 100-1000 seconds
SLEEP_RANGE=900
let "stime=($RANDOM%$SLEEP_RANGE) + 100"
echo sleeping for $stime seconds...
sleep $stime

