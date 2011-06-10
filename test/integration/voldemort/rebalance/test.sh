#!/bin/bash
source setup_env.inc

  $vldm_dir/bin/voldemort-rebalance.sh --current-cluster $vldm_dir/config/test_config1/config/cluster.xml --current-stores $vldm_dir/config/test_config1/config/stores.xml --target-cluster $work_dir/targe-cluster.xml 

