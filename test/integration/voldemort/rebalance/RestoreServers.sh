#!/bin/bash                                                                                            

source setup_env.inc
# copy the original cluster.xml to the existing nodes                                                  
cp $work_dir/original_cluster.xml ${TESTCFG_PREFIX}1/config/cluster.xml
cp $work_dir/original_cluster.xml ${TESTCFG_PREFIX}2/config/cluster.xml

# copy the cluster.xml for the new node into his directory                                             
cp $work_dir/new_cluster.xml ${TESTCFG_PREFIX}3/config/cluster.xml
