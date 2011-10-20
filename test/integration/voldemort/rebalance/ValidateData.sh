#!/bin/bash

source setup_env.inc
LOGDIR=$WORKDIR/log
DATADIR=$WORKDIR/data
LOGFILE=validation.log.`date +%H%M%S`
MSG="found - 100.0"

cd $VLDMDIR
bin/voldemort-rebalance.sh --output-dir $DATADIR --current-cluster $METADIR/final-cluster.xml --current-stores $METADIR/stores.xml --entropy true --verbose-logging > $LOGDIR/$LOGFILE

cd $WORKDIR
$WORKDIR/WaitforOutput.sh "$MSG" $LOGDIR/$LOGFILE
let EXITCODE="$?"
if [ "$EXITCODE" -ne "0" ]
then
  echo "Data validation failed! Check $LOGDIR/$LOGFILE for details!"
  exit $EXITCODE
fi

