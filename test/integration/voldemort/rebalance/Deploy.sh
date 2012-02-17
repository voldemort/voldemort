#!/bin/bash

set -x
source setup_env.inc

if [ "$1" == "" ]; then
  echo "Please specify a voldemort version to deploy"
  exit 0
fi

VOLDEMORT_FILE=voldemort-$1
VOLDEMORT_TAR=$VOLDEMORT_FILE.tar.gz

for ((i=0; i < ${TOTAL_NUM_SERVERS} ; i++)); do
  scp $VLDMDIR/dist/$VOLDEMORT_TAR $REMOTEUSER@${SERVER_MACHINES[$i]}:~/

  if [ "$REMOTE_RUN" == "true" ]; then
    export remote_call="ssh $REMOTEUSER@${SERVER_MACHINES[$i]}"
  else
    export remote_call=eval
  fi
  $remote_call "rm -rf voldemort; tar -xvzf ${VOLDEMORT_TAR}; mv ${VOLDEMORT_FILE} voldemort;"
  $remote_call "cd $REMOTEWORK; source setup_env.inc; chmod a+x \${WORKDIR}/*.sh"
done
