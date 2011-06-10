#!/bin/bash

source setup_env.inc

$WORKDIR/StartServer.sh 0
$WORKDIR/StartServer.sh 1

if [ "$1" = "all" ]
then 
  $WORKDIR/StartServer.sh 2
fi

$WORKDIR/RestoreMetadata.sh 0
$WORKDIR/RestoreMetadata.sh 1

if [ "$1" = "all" ]
then 
  $WORKDIR/RestoreMetadata.sh 2  
fi

