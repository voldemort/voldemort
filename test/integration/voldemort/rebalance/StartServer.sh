#!/bin/bash

source setup_env.inc
LOGDIR=$WORKDIR/log
terminal_string="Startup completed in"
server[0]="server1.log"
server[1]="server2.log"
server[2]="server3.log"
let CONFIGID=$1+1
CONFIGFILE=${TESTCFG_PREFIX}"$CONFIGID"
LOGFILE=${server[$1]}.`date +%H%M%S`

REMOTE_MACHINE_ID=$CONFIGID
if [ "$REMOTE_RUN" == "true" ]; then
  export remote_call="ssh ${SERVER_MACHINES[$REMOTE_MACHINE_ID]}"
else
  export remote_call=eval
fi
cd $VLDMDIR
$remote_call "cd $VLDMDIR; bin/voldemort-server.sh $CONFIGFILE > $LOGDIR/$LOGFILE" &
bash -x $WORKDIR/WaitforOutput.sh "$terminal_string" $LOGDIR/$LOGFILE
