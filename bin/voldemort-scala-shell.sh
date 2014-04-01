#!/bin/bash

# Shell script to launch Voldemort's scala shell
base_dir=$(dirname $0)/..
$base_dir/bin/run-class.sh voldemort.VoldemortScalaShell $@
