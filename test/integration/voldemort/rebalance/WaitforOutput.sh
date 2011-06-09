#!/bin/bash

grep "$1" $2 > /dev/null 2>&1
while [ "$?" -ne "0" ]
do
  sleep 3
  grep "$1" $2 > /dev/null 2>&1
done