#!/bin/bash

#
#   Copyright 2008-2013 LinkedIn, Inc
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
#

#
# Spin up the voldemort server(BDB-JE) with a production worthy JVM config
#
export VOLD_OPTS=" -server -Xms32684m -Xmx32684m -XX:NewSize=2048m -XX:MaxNewSize=2048m -XX:+UseConcMarkSweepGC -XX:+UseParNewGC -XX:CMSInitiatingOccupancyFraction=70 -XX:SurvivorRatio=2 -XX:+AlwaysPreTouch -XX:+UseCompressedOops -XX:+PrintTenuringDistribution -XX:+PrintGCDetails -XX:+PrintGCDateStamps -Xloggc:gc.log -XX:+PrintGCApplicationStoppedTime -XX:+PrintGCApplicationConcurrentTime "
echo "WARNING: This setup spins up a kickass server for production/perftesting. Make SURE that you are running on a server class machine"
echo "Tip: Make sure you either disable swap or give your user permission to mlock the heap"
bin/voldemort-server.sh $@
