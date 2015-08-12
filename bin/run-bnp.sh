#!/bin/bash

base_dir=$(cd $(dirname $0)/.. && pwd)

HADOOP_CONFIG_PATH_DEFAULT=/etc/hadoop/conf/

if [[ $# < 1 ]]; then
	echo "Usage: $0 config_file [hadoop_config_path]"
	echo ""
	echo "config_file : The file containing necessary config values, in key=value format."
	echo "hadoop_config_path : The path for *-site.xml files. Defaults to $HADOOP_CONFIG_PATH_DEFAULT"
	exit 1
fi

CONFIG_FILE=$1

if [[ $# > 1 ]]; then
	HADOOP_CONFIG_PATH=$2
else
	HADOOP_CONFIG_PATH=$HADOOP_CONFIG_PATH_DEFAULT
fi

VERSION=`grep 'curr.release' $base_dir/gradle.properties | sed -e 's/curr.release=\(.*\)/\1/'`
echo "Voldemort version detected: $VERSION"
echo "Executing BnP with:"
echo "config_file : $CONFIG_FILE"
echo "hadoop_config_path : $HADOOP_CONFIG_PATH"

# The jar file's name depends on the directory name...
export VOLDEMORT_JAR="$(echo $base_dir/build/libs/*-$VERSION-bnp.jar)"
export HADOOP_CLASSPATH=$HADOOP_CONFIG_PATH
hadoop jar $VOLDEMORT_JAR voldemort.store.readonly.mr.azkaban.VoldemortBuildAndPushJobRunner $CONFIG_FILE

echo "BnP run script finished!"
