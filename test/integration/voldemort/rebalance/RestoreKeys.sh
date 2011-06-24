#!/bin/bash

source setup_env.inc
LOGDIR=$WORKDIR/log
DATADIR=$WORKDIR/data

rm -rf $DATADIR
mkdir $DATADIR

cd $VLDMDIR
bin/voldemort-rebalance.sh --output-dir $DATADIR --current-cluster $WORKDIR/less-nodes-cluster.xml --current-stores $WORKDIR/stores.xml --entropy false --keys 10000
