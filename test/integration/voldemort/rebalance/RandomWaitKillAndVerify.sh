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

# sleep for 10-40 seconds
SLEEP_RANGE=30
let "stime=($RANDOM%$SLEEP_RANGE) + 10"
echo sleeping for $stime seconds...
sleep stime

read -p "Press any key to continue..."

# grep for completion string in the output   
grep "${TERMSTRING}" $LOGDIR/$LOGFILE > /dev/null 2>&1
if [ "$?" -eq 0 ]
then
  echo rebalancing finished!!!!!!
  exit 0
fi

# randomly choose servers to kill
# TODO: kill more than one server
NUM_SERVERS=3
let "tokill=($RANDOM%$NUM_SERVERS)"
echo killing servers ${servers[$tokill]}

kill `ps -ef | grep "${servers[$tokill]}" | grep -v grep | awk '{print $2}'`

# wait for rebalancing to terminate
echo waiting for rebalancing process to terminate...
$WORKDIR/WaitforOutput.sh "$ERROR_MSG" $LOGDIR/$LOGFILE

# check for rollbacked state on good servers and clear
# their rebalancing state if the check passes.
echo checking for server state
bash -x $WORKDIR/CheckAndRestoreMetadata.sh $tokill
# exit if validation check failed
if [ "$?" -ne "0" ]
then
  exit "$?"
fi

# restore metadata on killed servers so we can continue
echo resume killed server...
bash -x $WORKDIR/StartServer.sh $tokill
bash -x $WORKDIR/RestoreMetadata.sh $tokill

# check if entries are at the right nodes
bash -x $WORKDIR/ValidateData.sh
# exit if validation check failed
let EXITCODE="$?"
if [ "$EXITCODE" -ne "0" ]
then
  echo "Data validation failed! Check $LOGDIR/$LOGFILE for details!"
  exit $EXITCODE
fi
