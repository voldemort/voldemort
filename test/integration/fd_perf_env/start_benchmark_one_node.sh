#!/bin/bash

set -x
source setup_env.inc

[ "$1" == "" ] && echo "Usage: start_benchmark_one_node.sh <benchmark-name> <server> <bootstrap-url>" && exit -1
[ "$2" == "" ] && echo "Usage: start_benchmark_one_node.sh <benchmark-name> <server> <bootstrap-url>" && exit -1
[ "$3" == "" ] && echo "Usage: start_benchmark_one_node.sh <benchmark-name> <server> <bootstrap-url>" && exit -1

BENCHMARK_NAME=$1
SERVER=$2
bootstrap_url=$3

LOGDIR=/tmp/runlog
mkdir "${LOGDIR}"
server=$SERVER
LOGFILE=${server}.${BENCHMARK_NAME}.`date +%H%M%S`
export remote_call="ssh ${server}"
[ "$bootstrap_node" == "" ] && bootstrap_node=${server}

echo; echo "*************************************** Running benchmark on $server ***************************************"
$remote_call "cd ~/voldemort; bin/voldemort-performance-tool.sh --record-count 1000000 --value-size 1024 --url ${bootstrap_url} --store-name test --ops-count 100000000 -w 50 -r 50 --interval 5 --threads 100" > ${LOGDIR}/${LOGFILE} 2>&1 &
echo "Test running on server : $server"
echo "Logs collected at ${LOGDIR}/${LOGFILE}"

