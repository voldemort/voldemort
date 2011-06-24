#!/bin/bash                                                                                            

source setup_env.inc

# copy the original cluster.xml to the existing nodes                                                  
let i=1
while [ $TOTAL_NUM_SERVERS -gt $i ]
do 
  let SERVERID=$i-1
  scp $work_dir/original_cluster.xml $REMOTEUSER@${SERVER_MACHINES[$SERVERID]}:${TESTCFG_PREFIX}$i/config/cluster.xml
  let i+=1
done

# copy the cluster.xml for the new node into his directory                     
# assuming the last one is the new node                        
scp $work_dir/new_cluster.xml $REMOTEUSER@${SERVER_MACHINES[$SERVERID]}:${TESTCFG_PREFIX}$i/config/cluster.xml
