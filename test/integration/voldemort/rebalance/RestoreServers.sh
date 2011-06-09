#!/bin/bash                                                                                            

vldm_dir=/Users/lgao/Projects/voldemort
work_dir=/Users/lgao/work/rebalance

# copy the original cluster.xml to the existing nodes                                                  
cp $work_dir/original_cluster.xml $vldm_dir/config/test_config1/config/cluster.xml
cp $work_dir/original_cluster.xml $vldm_dir/config/test_config2/config/cluster.xml

# copy the cluster.xml for the new node into his directory                                             
cp $work_dir/new_cluster.xml $vldm_dir/config/test_config3/config/cluster.xml
