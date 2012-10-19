#!/bin/bash -e

# Copyright 2012 LinkedIn, Inc
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may not
# use this file except in compliance with the License. You may obtain a copy of
# the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations under
# the License.

usage()  {
    echo
    echo "Usage:"
    echo "bin/repeat-junit.sh num_times"
    echo 
    cat <<EOF 
Invoke bin/repeat-junit.sh from the root of a Voldemort
checkout. bin/repeat-junit.sh invokes 'ant junit' num_times. 

The argument num_times must be an integer. 

The pretty html junit output that ends up in dist/junit-reports on a
single invocation of 'ant junit' is collected in a temp
directory. This circumvents the normal behavior of ant in which
dist/junit-reports is overwritten with each invocation of 'ant
junit'. 

bin/repeat-junit.sh is useful to run after making some substantial
changes, or when trying to track down intermittent failures (that
occur more on your local box then on a Hudson test machine...).
EOF
}

if [ $# != 1 ]; then
    echo "ERROR: Incorrect number of arguments: $# provided, 1 needed." >&2
    usage
    exit 1
fi

NUMTIMES=$1
if [[ ! $NUMTIMES == +([0-9]) ]]
then
   echo "ERROR: argument num_times is not an integer: $NUMTIMES." >&2
   usage
   exit 1
fi

TMPDIR=`mktemp -d -p '/tmp/'`

for ((i=1;i<=$NUMTIMES;i++)); do 
    echo
    echo "STARTING ITERATION $i"
    echo

    # Run junit and capture stdout to .out and stderr to .err
    junitiout="$TMPDIR/junit-$i.out"
    junitierr="$TMPDIR/junit-$i.err"
    ant junit > >(tee $junitiout) 2> >(tee $junitierr >&2)

    # Collect results
    junitidir="$TMPDIR/junit-reports-$i"
    echo
    echo "COLLECTING RESULTS OF ITERATION $i IN $junitidir"
    cp -r dist/junit-reports $junitidir
    mv $junitiout $junitidir
    mv $junitierr $junitidir
done


