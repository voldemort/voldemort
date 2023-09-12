#!/bin/bash

base_dir=$(cd $(dirname $0)/.. && pwd)

VERSION=`grep 'curr.release' $base_dir/gradle.properties | sed -e 's/curr.release=\(.*\)/\1/'`
echo "Voldemort version detected: $VERSION"

# The jar file's name depends on the directory name...
export VOLDEMORT_JAR="$(echo $base_dir/build/libs/voldemort-$VERSION.jar)"

$base_dir/bin/run-class.sh voldemort.store.readonly.ReadOnlyFileDiffer $@
