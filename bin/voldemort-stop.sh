#!/bin/bash

#
#   Copyright 2008-2009 bebo, Inc
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#   you may not use this file except in compliance with the License.
#   You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
#

usage="Usage: voldemort-stop.sh"

bin=`dirname "$0"`
bin=`cd "$bin"; pwd`

if [ "$#" != "0" ]
then
	echo $usage
	exit 1
fi

pids=`ps xwww | grep voldemort.server.VoldemortServe[r] | awk '{print $1}'`

if [ "$pids" != "" ]
then
	echo $(hostname)': Stopping Voldemort...'
	kill $pids
	exit 0
fi 

echo $(hostname)': Voldemort Server not running!'
exit 1
