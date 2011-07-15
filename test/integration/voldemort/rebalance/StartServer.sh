#!/bin/bash

source setup_env.inc
LOGDIR=$WORKDIR/log
terminal_string="Startup"
let CONFIGID=$1+1
LOGFILE=server$CONFIGID.`date +%H%M%S`

REMOTE_MACHINE_ID=$1
if [ "$REMOTE_RUN" == "true" ]; then
  export remote_call="ssh $REMOTEUSER@${SERVER_MACHINES[$REMOTE_MACHINE_ID]}"
else
  export remote_call=eval
fi

$remote_call "source .bash_profile; cd $REMOTEWORK; source setup_env.inc; cd \$VLDMDIR; bin/voldemort-server.sh \${TESTCFG_PREFIX}"$CONFIGID" > \$LOGDIR/$LOGFILE" &
$remote_call "cd $REMOTEWORK; source setup_env.inc; \$WORKDIR/WaitforOutput.sh \"$terminal_string\" \$LOGDIR/$LOGFILE"

