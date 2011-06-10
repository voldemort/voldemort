#!/bin/bash

source setup_env.inc

LOGDIR=$WORKDIR/log
LOGFILE=$1
ERROR_MSG="ERROR Unsuccessfully terminated rebalance operation"
TERMSTRING="Successfully terminated rebalance all tasks"
SERVSTRING="VoldemortServer $TESTCFG_PREFIX"
servers[0]="$SERVSTRING"1
servers[1]="$SERVSTRING"2
servers[2]="$SERVSTRING"3

# sleep for 1-10 seconds
SLEEP_RANGE=10
let "stime=($RANDOM%$SLEEP_RANGE) + 1"
echo sleeping for $stime seconds...
sleep $stime

# read -p "Press any key to continue..."

# grep for completion string in the output   
grep "${TERMSTRING}" $LOGDIR/$LOGFILE > /dev/null 2>&1
if [ "$?" -eq 0 ]
then
  echo rebalancing finished!!!!!!
  exit 9 
fi

# randomly choose servers to kill
# TODO: kill more than one server
NUM_SERVERS=3
#NUMS_TO_KILL_OR_SUSPEND=$(($RANDOM%$NUM_SERVERS+1))
NUMS_TO_KILL_OR_SUSPEND=1
# the
tokill_array=(`python -c "import random; print ' '.join([str(x) for x in random.sample(range(${NUM_SERVERS}),${NUMS_TO_KILL_OR_SUSPEND})])"`)
# 
kill_or_suspend_array=(`python -c "import random; print ' '.join([random.choice(['kill','suspend']) for i in range(${NUMS_TO_KILL_OR_SUSPEND})])"`)
to_kill_pids=()

for ((i=0; i < ${NUMS_TO_KILL_OR_SUSPEND} ; i++)); do
  to_kill=${tokill_array[$i]}
  pid_to_kill=`ps -ef | grep "${servers[$tokill]}" | grep -v grep | awk '{print $2}'`
  to_kill_pids[$i]=$pid_to_kill
  if [[ "${kill_or_suspend_array[$i]}" == "kill" ]]
  then
    echo killing servers ${servers[$tokill]}
    kill $pid_to_kill
  else
    echo suspending servers ${servers[$tokill]}
    kill -STOP $pid_to_kill
  fi
done

# TODO: What to do here if there are no kill?
# wait for rebalancing to terminate
echo waiting for rebalancing process to terminate...
$WORKDIR/WaitforOutput.sh "$ERROR_MSG" $LOGDIR/$LOGFILE

# restore metadata on killed servers so we can continue
for ((i=0; i < ${NUMS_TO_KILL_OR_SUSPEND} ; i++)); do
  to_kill=${tokill_array[$i]}
  pid_to_kill=${to_kill_pids[$i]}
  # check for rollbacked state on good servers and clear
  # their rebalancing state if the check passes.
  echo checking for server state
  bash -x $WORKDIR/CheckAndRestoreMetadata.sh $tokill
  # exit if validation check failed
  if [ "$?" -ne "0" ]
  then
    echo "Metadata validation failed! Check files in $TMPCLUSTER for details!"
    exit "$?"
  fi

  echo resume killed server...
  if [[ "${kill_or_suspend_array[$i]}" == "kill" ]]; then
    echo restart servers ${servers[$tokill]}
    bash -x $WORKDIR/StartServer.sh $tokill
    bash -x $WORKDIR/RestoreMetadata.sh $tokill
  else
    echo resume servers ${servers[$tokill]}
    kill -CONT $pid_to_kill
  fi
done

# check if entries are at the right nodes
bash -x $WORKDIR/ValidateData.sh
# exit if validation check failed
let EXITCODE="$?"
if [ "$EXITCODE" -ne "0" ]
then
  echo "Data validation failed! Check $LOGDIR/$LOGFILE for details!"
  exit $EXITCODE
fi
