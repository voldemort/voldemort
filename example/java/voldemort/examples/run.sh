#!/bin/bash
set -e

CUR_DIR=$(cd $(dirname ${BASH_SOURCE}[0]) && pwd)
PROJ_ROOT=$(cd ${CUR_DIR}/../../../.. && pwd)

function get_cp
{
  for file in ${PROJ_ROOT}/lib/*.jar ${PROJ_ROOT}/dist/*.jar;
  do
    CLASSPATH=$CLASSPATH:$file
  done
  CLASSPATH=${CLASSPATH}:${PROJ_ROOT}/dist/resources
  CLASSPATH=${CLASSPATH}:${CUR_DIR}/../..
  export CLASSPATH
}

get_cp

java -cp ${CLASSPATH} voldemort.examples.ClientExample $@

