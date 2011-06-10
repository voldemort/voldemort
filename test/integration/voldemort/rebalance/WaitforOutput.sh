#!/bin/bash

if [ "$remote_call" == "" ]; then
  remote_call=eval
fi
grep_str=$1

$remote_call "grep '$grep_str' $2" > /dev/null 2>&1
while [ "$?" -ne "0" ]
do
  sleep 3
  $remote_call "grep '$grep_str' $2" > /dev/null 2>&1
done
