#!/bin/bash

set -x
source setup_env.inc

if [ "$1" == "" ]; then
  echo "Please specify a name for the type of benchmark"
  exit 0
fi

BENCHMARK_NAME=$1
bootstrap_node=""

LOGDIR=/tmp/runlog
mkdir "${LOGDIR}"

for server in `grep host ${CONFIG_DIR}/config/cluster.xml | cut -d">" -f2 | cut -d"<" -f1`
do 
  LOGFILE=${server}.${BENCHMARK_NAME}.`date +%H%M%S`
  export remote_call="ssh ${server}"
  [ "$bootstrap_node" == "" ] && bootstrap_node=${server}

  echo; echo "*************************************** Running benchmark on $server ***************************************"
  $remote_call "cd ~/voldemort; bin/voldemort-performance-tool.sh --record-count 1000000 --value-size 1024 --url tcp://${bootstrap_node}:6666 --store-name test --ops-count 100000000 -w 50 -r 50 --interval 5 --threads 30" > ${LOGDIR}/${LOGFILE} 2>&1 &
  echo "Test running on server : $server"
  echo "Logs collected at ${LOGDIR}/${LOGFILE}"
done

