#!/bin/bash

source setup_env.inc
LOGDIR=$WORKDIR/log
DATADIR=$WORKDIR/data

rm -f $DATADIR/*

cd $VLDMDIR
bin/voldemort-rebalance.sh --output-dir $DATADIR --current-cluster config/test_config1/config/cluster.xml --current-stores config/test_config1/config/stores.xml --entropy false
