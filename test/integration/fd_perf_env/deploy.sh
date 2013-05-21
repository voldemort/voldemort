#!/bin/bash

set -x
source setup_env.inc

if [ "$1" == "" ]; then
  echo "Please specify a voldemort version to deploy"
  exit 0
fi

ANNHILATE="FALSE"
if [ "$2" == "annhilate" ]; then
  echo "Setting Annhilate property to true"
  ANNHILATE="TRUE"
fi

VOLDEMORT_FILE=voldemort-$1
VOLDEMORT_TAR=$VOLDEMORT_FILE.tar.gz
CONFIG_DIR_NAME=`basename ${CONFIG_DIR}`
CONFIG_TARBALL_NAME=${CONFIG_DIR_NAME}.tar.gz
CONFIG_TARBALL_PATH=/tmp/${CONFIG_TARBALL_NAME}

rm -f ${CONFIG_TARBALL_PATH}
cd ${CONFIG_DIR}/..; 
tar -czf ${CONFIG_TARBALL_PATH} ${CONFIG_DIR_NAME}
echo "Created the config tarball here: ${CONFIG_TARBALL_PATH}"
ls ${CONFIG_TARBALL_PATH}
cd -;

nodeid=0
for server in `grep host ${CONFIG_DIR}/config/cluster.xml | cut -d">" -f2 | cut -d"<" -f1`
do 
  LOGFILE=${server}.`date +%H%M%S`
  export remote_call="ssh ${server}"

  echo; echo "*************************************** Deploying on $server ***************************************"
  echo "Stopping existing server on $server ..."
  $remote_call "pkill -9 -f 'java.*voldemort'" > /dev/null 2>&1 

  if [ "$ANNHILATE" = "TRUE" ]
  then
    echo "Annhilating config and data on $server ..."
    $remote_call "rm -rf /export/home/eng/csoman/*" > /dev/null 2>&1
    scp ${CONFIG_TARBALL_PATH} ${server}: 
    $remote_call "rm -rf config; tar -xzf ${CONFIG_TARBALL_NAME}; mv ${CONFIG_DIR_NAME} config;" > /dev/null 2>&1
    $remote_call "cp config/server.properties config/config/; echo 'node.id=${nodeid}' >> config/config/server.properties;" > /dev/null 2>&1
  fi

  echo; echo "Copying package to server : ${server} ..."
  scp $VLDMDIR/dist/$VOLDEMORT_TAR ${server}:  > /dev/null 2>&1

  echo; echo "Extracting on : ${server} ... " 
  $remote_call "rm -rf voldemort; tar -xzf ${VOLDEMORT_TAR}; mv ${VOLDEMORT_FILE} voldemort;" > /dev/null 2>&1

  echo; echo "Starting Voldemort service on : ${server} ..."
  $remote_call "mkdir ${LOGDIR}" > /dev/null 2>&1
  $remote_call "cd ~/voldemort; bin/voldemort-server.sh ~/config > ${LOGDIR}/${LOGFILE} 2>&1 &" > /tmp/${server}-deploy.log 2>&1 &
  echo "Voldemort service running on server : $server"
  echo "Logs collected at ${LOGDIR}/${LOGFILE}"

  (( nodeid++ ))
done

sleep 2

for server in `grep host ${CONFIG_DIR}/config/cluster.xml | cut -d">" -f2 | cut -d"<" -f1`
do
  export remote_call="ssh ${server}"
  $remote_call "ps -ef | grep vold"
done
