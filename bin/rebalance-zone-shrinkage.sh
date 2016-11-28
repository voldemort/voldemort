#!/bin/bash -e

#
#   Copyright 2013 LinkedIn, Inc
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.

# This script generates a final cluster.xml and stores.xml after dropping a zone from the current
# cluster.xml and stores.xml, placing the output in output directory.

# Argument = -c <current_cluster.xml> -s <current_stores.xml> -d <drop_zoneid> -o <outputdir>
#
# This script steals partitions from other nodes in the zone that is being dropped 
# and assigns them to the nodes in the surviving zones.

# This script uses getopts which means only single character switches are allowed.
# Using getopt would allow for multi character switch names but would come at a
# cost of not being cross compatible.

# Function to display usage
usage_and_exit() {
  echo "ERROR: $1."
  cat <<EOF
  
  Usage: $0 options 
  OPTIONS:
   -h     Show this message
   -c     Current cluster that describes the cluster
   -s     Current stores.xml for the cluster
   -d     ZoneId that you want to drop
   -o     Output dir for final cluster.xml/stores.xml
EOF
exit 1
}

# initialize  variables to an empty string
current_cluster=""
current_stores=""
drop_zoneid=""
output_dir=""

# Figure out voldemort home directory
dir="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
vold_home="$(dirname "$dir")"

# Parse options
while getopts "hc:s:d:o:" OPTION
do
  case $OPTION in
  h)
    usage_and_exit
    exit 1
    ;;
  c)
    current_cluster=$OPTARG
    echo "[rebalance-zone-shrinkage] Will rebalance on the cluster described in '$current_cluster'."
    ;;
  s)
    current_stores=$OPTARG
    echo "[rebalance-zone-shrinkage] Will rebalance stores described in '$current_stores'."
    ;;    
  d)
    drop_zoneid=$OPTARG
    echo "[rebalance-zone-shrinkage] Will get rid of servers in zone '$drop_zoneid'."
    ;;
  o)
    output_dir=$OPTARG
    mkdir -p $output_dir
    echo "[rebalance-zone-shrinkage] Using '$output_dir' for final files generated."
    ;;
  ?)
    usage_and_exit
  ;;
     esac
done

if [[ -z $current_cluster ]] || [[ -z $drop_zoneid ]] || [[ -z $output_dir ]] || [[ -z $current_stores ]]
then
     printf "\n"
     echo "[rebalance-zone-shrinkage] Missing argument. Check again."
     usage_and_exit
     exit 1
fi

if [ ! -e $current_cluster ]; then
    usage_and_exit "File '$current_cluster' does not exist."
fi

if [ ! -e $current_stores ]; then
    usage_and_exit "File '$current_stores' does not exist."
fi

mkdir -p $output_dir
$vold_home/bin/run-class.sh voldemort.tools.ZoneClipperCLI \
                            --current-cluster $current_cluster \
                            --current-stores $current_stores \
                            --drop-zoneid $drop_zoneid \
                            --output-dir $output_dir/

