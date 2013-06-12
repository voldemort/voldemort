#!/bin/bash -e

#
#   Copyright 2013 LinkedIn, Inc
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

# This script generates a final-cluster.xml for spinning up a new cluster.
# Argument = -v vold_home -c current_cluster -s current_stores -o output dir
# The final cluster is placed in output_dir/

# This script uses getopts which means only single character switches are allowed.
# Using getopt would allow for multi charcter switch names but would come at a
# cost of not being cross compatibility.

# Function to display usage
usage_and_exit() {
  echo "ERROR: $1."
  cat <<EOF

  Usage: $0 options
  OPTIONS:
   -h     Show this message
   -v     Path to Voldemort
   -c     Current Cluster that desribes the cluster.
   -s     Current Stores that desribes the store. If you do not have info about the stores yet, use
          'config/tools/dummy-stores.xml' from the gitli repo.
   -o     Output dir where all interim and final files will be stored.
          The directory will be created if it does not exist yet.
EOF
exit 1
}

# initialize  varibles to an empty string
vold_home=""
current_cluster=""
current_stores=""
output_dir=""

# Parse options
while getopts “hv:c:s:o:” OPTION
do
  case $OPTION in
  h)
    usage_and_exit
    exit 1
    ;;
  v)
    vold_home=$OPTARG
   echo "[rebalance-new-cluster] Voldemort home='$vold_home' "
    ;;
  c)
    current_cluster=$OPTARG
    echo "[rebalance-new-cluster] Will rebalance on the cluster described in '$current_cluster'."
    ;;
  s)
    current_stores=$OPTARG
    echo "[rebalance-new-cluster] Will rebalance on the stores described in '$current_stores'."
    ;;
  o)
    output_dir=$OPTARG
    mkdir -p $output_dir
    echo "[rebalance-new-cluster] Using '$output_dir' for all interim and final files generated."
    ;;
  ?)
    usage_and_exit
  ;;
     esac
done

if [[ -z $vold_home ]] || [[ -z $current_cluster ]] || [[ -z $current_stores ]] \
    || [[ -z $output_dir ]]
then
     printf "\n"
     echo "[rebalance-new-cluster] Missing argument. Check again."
     usage_and_exit
     exit 1
fi

if [ ! -d $vold_home ]; then
    usage_and_exit "Directory '$vold_home' does not exist."
fi

if [ ! -e $current_cluster ]; then
    usage_and_exit "File '$current_cluster' does not exist."
fi

if [ ! -e $current_stores ]; then
    usage_and_exit "File '$current_stores' does not exist."
fi

# The final cluster.xml for new cluster is generated in three steps.
# Step 1 : Repartitioner is executed to limit the max contiguous partition. Four such
#          runs are attempted and the best cluster.xml from this step is piped to step 2.
# Step 2: Cluster.xml from step 1 is fed to the repartitioner along with random swap
#         attempts. The repartitioner randomly swaps the partitions and tries to balance the ring.
# Step 3: Similar to step1, to improve the balance.
#

step2_swap_attempts=1000
step2_swap_successes=1000

# Step 1
mkdir -p $output_dir/step1/
$vold_home/bin/run-class.sh voldemort.tools.RepartitionerCLI \
                            --current-cluster $current_cluster \
                            --current-stores $current_stores \
                            --max-contiguous-partitions 3 \
                            --attempts 4 \
                            --output-dir $output_dir/step1/

if [ ! -e $output_dir/step1/final-cluster.xml ]; then
    usage_and_exit "File '$final-cluster.xml' does not exist."
fi

# Step 2
mkdir -p $output_dir/step2
$vold_home/bin/run-class.sh voldemort.tools.RepartitionerCLI \
                            --current-cluster $output_dir/step1/final-cluster.xml \
                            --current-stores $current_stores \
                            --output-dir $output_dir/step2/ \
                            --enable-random-swaps \
                            --random-swap-attempts $step2_swap_attempts \
                            --random-swap-successes $step2_swap_successes

if [ ! -e $output_dir/step2/final-cluster.xml ]; then
    usage_and_exit "File '$final-cluster.xml' does not exist."
fi

mkdir -p $output_dir/step3/
# Step 3
$vold_home/bin/run-class.sh voldemort.tools.RepartitionerCLI \
                            --current-cluster $output_dir/step2/final-cluster.xml \
                            --current-stores $current_stores \
                            --max-contiguous-partitions 3 \
                            --attempts 4 \
                            --output-dir $output_dir/step3/ \

echo "[rebalance-new-cluster] Placing final-cluster.xml in '$output_dir'"
cp $output_dir/step3/final-cluster.xml $output_dir/final-cluster.xml

