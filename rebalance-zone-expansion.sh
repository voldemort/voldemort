#!/bin/bash

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

# This script uses getopts which means only a single character switch are allowed.
# Using getopt would allow for multi charcter switch names but would come at a 
# cost of being cross compatibility.

# Argument = -v vold_home -c current_cluster -s current_stores -i interim_cluster -f final_stores
#            -o output dir

# Function to display usage
usage_and_exit() {
  cat <<EOF
  
  Usage: $0 options 
  OPTIONS:
   -h     Show this message
   -v     Path to Voldermort
   -c     Current Cluster that desribes the cluster
   -s     Current Stores that desribes the store
   -i     Interim Cluster that corresponds to zone expansion.
   -f     Final Stores that corresponds to zone expansion.
   -o     Output dir where all interim and final files will be stored.
EOF
exit 1
}

# initiliaze varibles to an empty string
vold_home=""
current_cluster=""
current_stores=""
interim_cluster=""
final_stores=""
output_dir=""

# Parse options
while getopts “hv:c:s:i:f:o:” OPTION
do
  case $OPTION in
  h)
    usage_and_exit
    exit 1
    ;;
  v)
    vold_home=$OPTARG
   echo "[rebalance-zone-expansion] Voldemort home='$vold_home' "
    ;;
  c)
    current_cluster=$OPTARG
    echo "[rebalance-zone-expansion] Will rebalance on the cluster described in '$current_cluster'."
    ;;
  s)
    current_stores=$OPTARG
    echo "[rebalance-zone-expansion] Will rebalance on the stores described in '$current_stores'."
    ;;
  i)
    interim_cluster=$OPTARG
    echo "[rebalance-zone-expansion] Will rebalance on the interim cluster described in '$interim_cluster'."
    ;;
  f)
    final_stores=$OPTARG
    echo "[rebalance-zone-expansion] Will rebalance on the final stores described in '$final_stores'."
    ;;
  o)
    output_dir=$OPTARG
    mkdir -p $output_dir
    echo "[rebalance-zone-expansion] Using '$output_dir' for all interim and final files generated."
    ;;
  ?)
    usage_and_exit
  ;;
     esac
done

if [[ -z $vold_home ]] || [[ -z $current_cluster ]] || [[ -z $current_stores ]] \
    || [[ -z $interim_cluster ]] || [[ -z $final_stores ]] || [[ -z $output_dir ]]
then
     printf "\n"
     echo "[rebalance-zone-expansion] Missing argument. Check again."
     usage_and_exit
     exit 1
fi

# The final cluster.xml for zone expansion is generated in three steps.
# Step 1 : In step 1 1000 separate ierations of the reparitioner is executed and the one 
#          with the minimal utility value is chose for step 2.
# Step 2: In step 2, the cluster.xml from step 1 is fed to the repartitioner along with random swap 
#         attempts. The repartitioner randomly swaps the partitions and tries to balance the ring.
# Step 3: Finally, a plan is generated on how to reach from the orignal cluster topology to
#         the one that is generated in step 2. 
#

# Step 1
for i in {1..1000} 
do    
    mkdir -p $output_dir/step1/$i
    $vold_home/bin/run-class.sh voldemort.tools.RepartitionerCLI \
                                --current-cluster $current_cluster \
                                --current-stores $current_stores \
                                --interim-cluster $interim_cluster \
                                --final-stores $final_stores \
                                --attempts 1 \
                                --output-dir $output_dir/step1/$i
done

#find the run with the best (minimal) utility value 
bestUtil=$(grep "Utility" $output_dir/step1/*/final-cluster.xml.analysis | sort -nk 3 | cut -f4 -d / | head -n 1)

# Step 2
$vold_home/bin/run-class.sh voldemort.tools.RepartitionerCLI \
                            --current-cluster $output_dir/step1/$bestUtil/final-cluster.xml \
                            --current-stores $final_stores \
                            --output-dir $output_dir/step2 \
                            --enable-random-swaps \
                            --attempts 5 \
                            --random-swap-attempts 1000 \

# Step 3
mkdir -p $output_dir/step3/
$vold_home/bin/run-class.sh voldemort.tools.RebalancePlanCLI \
                             --current-cluster $current_cluster \
                             --current-stores $current_stores \
                             --final-cluster $output_dir/step2/final-cluster.xml \
                             --final-stores $final_stores \
                             --output-dir $output_dir/step3/ | tee $output_dir/step3/plan.txt
                             
                             