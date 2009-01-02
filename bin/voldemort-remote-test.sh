#!/bin/bash

if [ $# -ne 3 ];
then
  echo 'USAGE: bin/voldemort-remote-test.sh bootstrap-url num-requests start-num'
  exit 1
fi

bin/run-wrapper.sh voldemort.performance.RemoteTest ${1} ${2} ${3}