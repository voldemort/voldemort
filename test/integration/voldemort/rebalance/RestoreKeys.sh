#!/bin/bash

source setup_env.inc
LOGDIR=$WORKDIR/log
DATADIR=$WORKDIR/data

rm -rf $DATADIR
mkdir $DATADIR

cd $VLDMDIR
bin/voldemort-rebalance.sh --output-dir $DATADIR --current-cluster $METADIR/initial-cluster.xml --current-stores $METADIR/stores.xml --entropy false --keys 10000
