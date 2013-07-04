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

# This script shuffles a current cluster.xml (to achieve a more balanced cluster) and then
# outputs a final-cluster.xml corresponding to it. It also outputs the plan and the cost
# to achieve this final cluster config. Eg.
#
# Argument = -c current_cluster -s current_stores -o output dir
# The final cluster is placed in output_dir/
#
# The script also exposes an optional argument -m <max contiguous partitons>.
# When invoked with the -m option the script first splits runs of partition that are bigger than
# <max contiguous partitons> and then repartitions the cluster.
#
# Eg. Argument = -c current_cluster -s current_stores -o output dir -m 3
#
# This script can be used again on its own output. That is, if first attempt only gets you half-way 
# to where you want to go in terms of repartitioning, then take output final-cluster xml and use as 
# input to this tool again.
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
   -c     Current cluster that describes the cluster.
   -s     Current stores that describes the store. If you do not have info about the stores yet, look
          under 'voldemort_home/config/tools/' for some store examples.
   -o     Output dir where all interim and final files will be stored.
          The directory will be created if it does not exist yet.
   -m     Max allowed contiguous partition IDs within a zone (optional argument)
EOF
exit 1
}

# initialize  variables
current_cluster=""
current_stores=""
output_dir=""
max_contiguous_partitions=-1

# Figure out voldemort home directory
dir="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
vold_home="$(dirname "$dir")"

# Parse options
while getopts “hc:s:o:m:” OPTION
do
  case $OPTION in
  h)
    usage_and_exit
    exit 1
    ;;
  c)
    current_cluster=$OPTARG
    echo "[rebalance-shuffle] Will rebalance on the cluster described in '$current_cluster'."
    ;;
  s)
    current_stores=$OPTARG
    echo "[rebalance-shuffle] Will rebalance on the stores described in '$current_stores'."
    ;;
  o)
    output_dir=$OPTARG
    mkdir -p $output_dir
    echo "[rebalance-shuffle] Using '$output_dir' for all interim and final files generated."
    ;;
  m)
    max_contiguous_partitions=$OPTARG
    echo "[rebalance-shuffle] Using '$max_contiguous_partitions' for max_contiguous_partitions."
    ;;
  ?)
    usage_and_exit
  ;;
     esac
done

if [[ -z $current_cluster ]] || [[ -z $current_stores ]] || [[ -z $output_dir ]]
then
     printf "\n"
     echo "[rebalance-shuffle] Missing argument. Check again."
     usage_and_exit
     exit 1
fi

if [ ! -e $current_cluster ]; then
    usage_and_exit "File '$current_cluster' does not exist."
fi

if [ ! -e $current_stores ]; then
    usage_and_exit "File '$current_stores' does not exist."
fi

# The final cluster.xml after shuffling is generated as described in the steps below:
# Step 0 : This step is executed only if the max_contiguous_partitions is passed to the 
#          script. In this step, lengthy runs of contiguous partitions are broken down
#          to a maximum run of max_contiguous_partitions value. The final-cluster.xml
#          from this step is then fed into step 1.
# Step 1: Current cluster.xml is fed to the repartitioner along with random swap
#         attempts. The repartitioner randomly swaps the partitions 
#         and tries to balance the ring.
# Step 2: A plan is generated on how to reach from the orignal cluster topology to
#         the one that is generated in step 1.
#
swap_attempts=250
max_contiguous_attempts=50
attempts=10
swap_successes=250

# Step 0
if [ "$max_contiguous_partitions" -ne "-1" ]; then
  mkdir -p $output_dir/step0/
  $vold_home/bin/run-class.sh voldemort.tools.RepartitionerCLI \
                            --current-cluster $current_cluster \
                            --current-stores $current_stores \
                            --max-contiguous-partitions $max_contiguous_partitions \
                            --attempts $max_contiguous_attempts \
                            --output-dir $output_dir/step0/

  if [ ! -e $output_dir/step0/final-cluster.xml ]; then
    usage_and_exit "File '$final-cluster.xml' does not exist."
  fi

  current_cluster=$output_dir/step0/final-cluster.xml
  echo "[rebalance-shuffle] Will rebalance on the cluster in '$current_cluster'."
fi

# Step 1
mkdir -p $output_dir/step1
$vold_home/bin/run-class.sh voldemort.tools.RepartitionerCLI \
                            --current-cluster $current_cluster \
                            --current-stores $current_stores \
                            --output-dir $output_dir/step1/ \
                            --enable-random-swaps \
                            --attempts $attempts \
                            --random-swap-attempts $swap_attempts \
                            --random-swap-successes $swap_successes

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
                             
echo "[rebalance-shuffle] Placing final-cluster.xml in '$output_dir'"
cp $output_dir/step2/final-cluster.xml $output_dir/final-cluster.xml
echo "[rebalance-shuffle] Placing plan.out in '$output_dir'"
cp $output_dir/step2/plan.out $output_dir/plan.out
echo "[rebalance-shuffle] Placing final-cluster.xml.analysis in '$output_dir'"
cp $output_dir/step1/final-cluster.xml.analysis $output_dir/final-cluster.xml.analysis
