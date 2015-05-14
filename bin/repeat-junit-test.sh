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
    echo "bin/repeat-junit-test.sh test_file num_times"
    echo 
    cat <<EOF 
Invoke bin/repeat-junit-test.sh from the root of a Voldemort
checkout. bin/repeat-junit-test.sh invokes 'ant junit-test' num_times
for test test_name.

The argument num_times must be an integer. The argument test_name must
be a class name suitable for 'ant junit-test'. I.e., a fully qualified
java class name. Remember, the class name does not include the .java
extension. An example test_name is voldemort.utils.ServerTestUtilsTest.

The pretty html junit output that ends up in dist/junit-single-report
on a single invocation of 'ant junit-test' is collected in a temp
directory. This circumvents the normal behavior of ant in which
dist/junit-single-report is overwritten with each invocation of 'ant
junit-test'.

bin/repeat-junit-test.sh is useful to run after adding a new test
case, or when trying to reproduce intermittent failures of a specific
test.
EOF
}

if [ $# != 2 ]; then
    echo "ERROR: Incorrect number of arguments: $# provided, 2 needed." >&2
    usage
    exit 1
fi

TESTNAME=$1
# Hackish test that makes sure some java file exists for given
# testname. No guarantee that junit-test can run the specified test,
# but at least protects against typos.
FILENAME=`echo $TESTNAME | sed 's/.*\.//g'`.java
FINDFILE=`find . -name "$FILENAME" | wc -l`
if [[ $FINDFILE == 0 ]]
then
   echo "ERROR: Did not find an appropriate file (with name $FILENAME), given test name $TESTNAME." >&2
   usage
   exit 1
fi


NUMTIMES=$2
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

    # Run junit-test and capture stdout to .out and stderr to .err
    junitiout="$TMPDIR/TEST-$TESTNAME-$i.out"
    junitierr="$TMPDIR/TEST-$TESTNAME-$i.err"
    ant junit-test -Dtest.name=$TESTNAME > >(tee $junitiout) 2> >(tee $junitierr >&2)

    # Collect results
    junitidir="$TMPDIR/junit-single-report-$TESTNAME-$i"
    echo
    echo "COLLECTING RESULTS OF ITERATION $i IN $junitidir"
    cp -r dist/junit-single-reports $junitidir
    mv $junitiout $junitidir
    mv $junitierr $junitidir
done


