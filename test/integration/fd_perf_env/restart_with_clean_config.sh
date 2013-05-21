#!/bin/bash

set -x
source setup_env.inc

VOLDEMORT_FILE=voldemort-$1
VOLDEMORT_TAR=$VOLDEMORT_FILE.tar.gz
CONFIG_DIR_NAME=`basename ${CONFIG_DIR}`
CONFIG_TARBALL=${CONFIG_DIR_NAME}.tar.gz
CONFIG_TARBALL_PATH=/tmp/${CONFIG_TARBALL}

nodeid=0
for server in `grep host ${CONFIG_DIR}/config/cluster.xml | cut -d">" -f2 | cut -d"<" -f1`
do 
  LOGFILE=${server}.`date +%H%M%S`
  export remote_call="ssh ${server}"

  echo; echo "*************************************** Deploying on $server ***************************************"
  echo "Stopping existing server on $server ..."
  $remote_call "pkill -9 -f 'java.*voldemort'" > /dev/null 2>&1 

  echo "Annhilating config on $server ..."
  $remote_call "rm -rf ${CONFIG_TARBALL}" > /dev/null 2>&1
  scp ${CONFIG_TARBALL_PATH} ${server}: > /dev/null 2>&1
  $remote_call "rm -rf config; tar -xzf ${CONFIG_TARBALL}; mv ${CONFIG_DIR_NAME} config;" > /dev/null 2>&1
  $remote_call "cp config/server.properties config/config/; echo 'node.id=${nodeid}' >> config/config/server.properties;" > /dev/null 2>&1

  echo; echo "Starting Voldemort service on : ${server} ..."
  $remote_call "mkdir ${LOGDIR}" > /dev/null 2>&1
  $remote_call "cd ~/voldemort; bin/voldemort-server.sh ~/config > ${LOGDIR}/${LOGFILE} 2>&1 &" > /tmp/${server}-deploy.log 2>&1 &
  echo "Voldemort service running on server : $server"
  echo "Logs collected at ${LOGDIR}/${LOGFILE}"

  (( nodeid++ ))
done

