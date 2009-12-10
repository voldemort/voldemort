#!/bin/bash

#
#   Copyright 2008-2009 LinkedIn, Inc
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

if [ $# -lt 2 ];
then
	echo 'USAGE: bin/voldemort-shell.sh currentCluster.xml targetCluster.xml stores.xml'
	exit 1
fi

base_dir=$(dirname $0)/..

$base_dir/bin/run-class.sh jline.ConsoleRunner voldemort.client.rebalance.RebalanceCommandShell $@
