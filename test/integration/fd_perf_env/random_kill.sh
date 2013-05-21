#!/bin/bash

set -x
source setup_env.inc

VOLDEMORT_FILE=voldemort-$1
VOLDEMORT_TAR=$VOLDEMORT_FILE.tar.gz
CONFIG_DIR_NAME=`basename ${CONFIG_DIR}`
CONFIG_TARBALL=${CONFIG_DIR_NAME}.tar.gz
CONFIG_TARBALL_PATH=/tmp/${CONFIG_TARBALL}

for ((;;))
do
  # sleep for 10-3600 seconds
  SLEEP_RANGE=2
  let "stime=($RANDOM%$SLEEP_RANGE) + 10"
  echo; echo "*************************************** Sleeping for $stime seconds ... ***************************************"
  sleep $stime
  
  nodeid=0
  NUMS_TO_KILL_OR_SUSPEND=1
  TOTAL_NUM_SERVERS=0
  SERVER_MACHINES=()
  for server in `grep host ${CONFIG_DIR}/config/cluster.xml | cut -d">" -f2 | cut -d"<" -f1`
  do 
    (( TOTAL_NUM_SERVERS++ ))
    SERVER_MACHINES[$nodeid]=$server
    (( nodeid++ ))
  done
  
  # for now, set it to one but it can be random
  tokill_array=(`python -c "import random; print ' '.join([str(x) for x in random.sample(range(${TOTAL_NUM_SERVERS}),${NUMS_TO_KILL_OR_SUSPEND})])"`)
  tokill=${tokill_array[0]}
  server=${SERVER_MACHINES[$tokill]}
  export remote_call="ssh ${SERVER_MACHINES[$tokill]}"
   
  LOGFILE=${server}.`date +%H%M%S` 
  echo; echo "Killing server $server"
  #$remote_call "pkill -9 -f 'java.*voldemort'" > /dev/null 2>&1 
  $remote_call "cd ~/voldemort; bin/voldemort-stop.sh;" 
  
  # Sleep for 5 seconds
#  echo; echo "Sleeping for 10 more seconds ..."
#  sleep 10 
  
  echo; echo "Starting Voldemort service on : ${server} ..."
  $remote_call "cd ~/voldemort; bin/voldemort-server.sh ~/config > ${LOGDIR}/${LOGFILE} 2>&1 &" > /tmp/${server}-deploy.log 2>&1 &
  echo "Voldemort service running on server : $server"
  echo "Logs collected at ${LOGDIR}/${LOGFILE}"
  
done
