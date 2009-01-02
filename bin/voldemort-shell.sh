#!/bin/bash

if [ $# != 2 ];
then
	echo 'USAGE: bin/voldemort-shell.sh store_name bootstrap_url [command_file]'
	exit 1
fi

base_dir=$(dirname $0)/..

$base_dir/bin/run-wrapper.sh voldemort.VoldemortClientShell ${1} ${2} ${3}