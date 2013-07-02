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

# This script generates a final cluster.xml and a plan for the cluster expansion.
# The final cluster is placed in output_dir/
# Argument = -c current_cluster -s current_stores -i interim_cluster -o output dir
#
# This script steals partitions from other nodes and assigns them to the new nodes
# in the cluster. While stealing partitions, the script doesn't make an effort to balance
# the cluster. However, because the repartitioner is invoked multiple distinct times, 
# the most balanced such expansion is chosen. #To further rebalance the cluster, 
# use the rebalance-shuffle script.
# 
# In a typical workflow one would chain this script with the rebalance-shuffle script.
# by running this script first and then the shuffle to achieve better balancing.
#
# This script uses getopts which means only single character switches are allowed.
# Using getopt would allow for multi charcter switch names but would come at a
# cost of not being cross compatible.

# Function to display usage
usage_and_exit() {
  echo "ERROR: $1."
  cat <<EOF
  
  Usage: $0 options 
  OPTIONS:
   -h     Show this message
   -c     Current cluster that describes the cluster
   -s     Current stores that describes the store. If you do not have info about the stores yet, look
          under 'voldemort_home/config/tools/' for some store examples.
   -i     Interim Cluster that corresponds to cluster expansion.
   -o     Output dir where all interim and final files will be stored.
EOF
exit 1
}

# initialize  variables to an empty string
current_cluster=""
current_stores=""
interim_cluster=""
output_dir=""

# Figure out voldemort home directory
dir="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
vold_home="$(dirname "$dir")"

# Parse options
while getopts “hc:s:i:o:” OPTION
do
  case $OPTION in
  h)
    usage_and_exit
    exit 1
    ;;
  c)
    current_cluster=$OPTARG
    echo "[rebalance-cluster-expansion] Will rebalance on the cluster described in '$current_cluster'."
    ;;
  s)
    current_stores=$OPTARG
    echo "[rebalance-cluster-expansion] Will rebalance on the stores described in '$current_stores'."
    ;;
  i)
    interim_cluster=$OPTARG
    echo "[rebalance-cluster-expansion] Will rebalance on the interim cluster described in '$interim_cluster'."
    ;;
  o)
    output_dir=$OPTARG
    mkdir -p $output_dir
    echo "[rebalance-cluster-expansion] Using '$output_dir' for all interim and final files generated."
    ;;
  ?)
    usage_and_exit
  ;;
     esac
done

if [[ -z $current_cluster ]] || [[ -z $current_stores ]] \
    || [[ -z $interim_cluster ]] || [[ -z $output_dir ]]
then
     printf "\n"
     echo "[rebalance-cluster-expansion] Missing argument. Check again."
     usage_and_exit
     exit 1
fi

if [ ! -e $current_cluster ]; then
    usage_and_exit "File '$current_cluster' does not exist."
fi

if [ ! -e $current_stores ]; then
    usage_and_exit "File '$current_stores' does not exist."
fi

if [ ! -e $interim_cluster ]; then
    usage_and_exit "File '$interim_cluster' does not exist."
fi


# The final cluster.xml for cluster expansion is generated in two steps.
# Step 1: Current cluster.xml is fed to the repartitioner along with the interim cluster xml.
#         The repartitioner tries to balance the ring by moving the partitions around.
# Step 2: A plan is generated on how to reach from the original cluster topology to
#         the one that is generated in step 1.
#

attempts=50

# Step 1
mkdir -p $output_dir/step1/
$vold_home/bin/run-class.sh voldemort.tools.RepartitionerCLI \
                            --current-cluster $current_cluster \
                            --current-stores $current_stores \
                            --interim-cluster $interim_cluster \
                            --attempts $attempts \
                            --output-dir $output_dir/step1/$i

if [ ! -e $output_dir/step1/final-cluster.xml ]; then
    usage_and_exit "File '$final-cluster.xml' does not exist."
fi

# Step 2
mkdir -p $output_dir/step2/
$vold_home/bin/run-class.sh voldemort.tools.RebalancePlanCLI \
                            --current-cluster $current_cluster \
                            --current-stores $current_stores \
                            --final-cluster $output_dir/step1/final-cluster.xml \
                            --output-dir $output_dir/step2/
                             
echo "[rebalance-cluster-expansion] Placing final-cluster.xml in '$output_dir'"
cp $output_dir/step2/final-cluster.xml $output_dir/final-cluster.xml
echo "[rebalance-cluster-expansion] Placing plan.out in '$output_dir'"
cp $output_dir/step2/plan.out $output_dir/plan.out
