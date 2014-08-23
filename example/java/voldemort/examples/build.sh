#!/bin/bash
set -e

CUR_DIR=$(cd $(dirname ${BASH_SOURCE}[0]) && pwd)
PROJ_ROOT=$(cd ${CUR_DIR}/../../../.. && pwd)

function build_v
{
  echo "Building Voldemort"
  cd ${PROJ_ROOT}
  ./gradlew jar
}

function build_v_if_needed
{
  echo "Checking if Voldemort Jar is available"
  if [ $(ls ${PROJ_ROOT}/dist | grep voldemort | wc -l) == 0 ];
  then
    build_v;
  else
    echo "Voldemort Jar(s) exist(s) in ${PROJ_ROOT}/dist"
  fi;
}

function build_example
{
  echo "Building Example"
  cd ${PROJ_ROOT};

  for file in lib/*.jar contrib/*/lib/*.jar dist/*.jar;
  do
    CLASSPATH=$CLASSPATH:$file
  done
  CLASSPATH=$CLASSPATH:dist/resources

  set -x
  javac -cp ${CLASSPATH} ${CUR_DIR}/ClientExample.java
  set +x
}

function print_usage
{
  echo "Format: $1 [<build-target>]"
  echo "Build targets:"
  echo "          <DEFAULT>   same as auto"
  echo "          auto        build voldemort if needed; then build example program"
  echo "          all         build voldemort (even if it has been built) and example program"
  echo "          vold        build voldemort (even if it has been built)"
  echo "          example     build example program only"
}

if [ $# == '0' ] || [ $1 == 'auto' ];
then
  build_v_if_needed;
  build_example;
elif [ $1 == 'all' ];
then
  build_v;
  build_example;
elif [ $1 == 'vold' ];
then
  build_v;
elif [ $1 == 'example' ];
then
  build_example;
else
  print_usage $0;
  exit 1;
fi
