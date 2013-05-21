#!/bin/bash

set -x
source setup_env.inc

USAGE="Usage: start_benchmark_one_node.sh <benchmark-name> <client-list> <bootstrap-url>"

[ "$1" == "" ] && echo $USAGE && exit -1
[ "$2" == "" ] && echo $USAGE && exit -1
[ "$3" == "" ] && echo $USAGE && exit -1

BENCHMARK_NAME=$1
CLIENT_LIST=$2
bootstrap_url=$3

LOGDIR=/tmp/runlog
mkdir "${LOGDIR}"
for server in `echo $CLIENT_LIST | tr -s ',' '\n'`
do
  LOGFILE=${server}.${BENCHMARK_NAME}.`date +%H%M%S`
  export remote_call="ssh ${server}"
  [ "$bootstrap_node" == "" ] && bootstrap_node=${server}
  
  echo; echo "*************************************** Running benchmark on $server ***************************************"
  $remote_call "pkill -9 -f 'java.*voldemort'"
  $remote_call "cd ~/voldemort; bin/voldemort-performance-tool.sh --record-count 1000000 --value-size 1024 --url ${bootstrap_url} --store-name test --ops-count 100000000 -w 50 -r 50 --interval 5 --threads 100 --num-connections-per-node 100 " > ${LOGDIR}/${LOGFILE} 2>&1 &
  echo "Test running on server : $server"
  echo "Logs collected at ${LOGDIR}/${LOGFILE}"
done
