#!/bin/bash

WORKDIR=/Users/lgao/work/rebalance
VLDMDIR=/Users/lgao/Projects/voldemort
LOGDIR=$WORKDIR/log
terminal_string="Startup completed in"
server[0]="server1.log"
server[1]="server2.log"
server[2]="server3.log"
let CONFIGID=$1+1
CONFIGFILE=config/test_config"$CONFIGID"
LOGFILE=${server[$1]}.`date +%H%M%S`

cd $VLDMDIR
$VLDMDIR/bin/voldemort-server.sh $VLDMDIR/$CONFIGFILE > $LOGDIR/$LOGFILE &
$WORKDIR/WaitforOutput.sh "$terminal_string" $LOGDIR/$LOGFILE
