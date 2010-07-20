#!/bin/bash
#
#   Copyright 2010 LinkedIn, Inc
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#   you may not use this file except in compliance with the License.
#   You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
#

# Currently assumes data is already present on the cluster

if [ $# -lt 3 ];
then 
	echo 'USAGE: graph.sh cluster_url store_name ops_count'
	exit 1
fi

# Read args
URL=$1
STORE_NAME=$2
NUM_OPS=$3

# File names
DATE=`date +%C%y_%m_%d_%H_%M_%S`
LAT_SIZE_IMG_FILE=${DATE}LatVsSize.png

WRITE_FILE=${DATE}SizeWrite.dat
READ_FILE=${DATE}SizeRead.dat
GNUPLOT_SCRIPT=${DATE}Gnuplot
TEMP_FILE=${DATE}temporary

base_dir=$(dirname $0)/../../../../

######### RUN TESTS ########

# WRITES
echo "Write tests"
for size in 1024 2048 4096 8192 16384 32768 65536
do
	$base_dir/bin/voldemort-performance-tool.sh -w 100 --record-count 0 --ops-count $NUM_OPS --store-name $STORE_NAME --value-size $size --url $URL > $TEMP_FILE 
	throughput=`grep -e 'benchmark\][[:space:]]Throughput' $TEMP_FILE | cut -d " " -f 2`
	latMedian=`grep -e 'writes\][[:space:]]Median' $TEMP_FILE | cut -d " " -f 2`
	lat99=`grep -e 'writes\][[:space:]]99th' $TEMP_FILE | cut -d " " -f 2`
	latAverage=`grep -e 'writes\][[:space:]]Average' $TEMP_FILE | cut -d " " -f 2`
	echo "$size $latMedian $lat99 $latAverage $throughput" >> $WRITE_FILE 
	echo "Done write - Value size - $size"
done

# READS
echo "Read tests"
for size in 1024 2048 4096 8192 16384 32768 65536
do
	$base_dir/bin/voldemort-performance-tool.sh -r 100 --record-count 0 --ops-count $NUM_OPS --store-name $STORE_NAME --value-size $size --url $URL > $TEMP_FILE 
	throughput=`grep -e 'benchmark\][[:space:]]Throughput' $TEMP_FILE | cut -d " " -f 2`
	latMedian=`grep -e 'reads\][[:space:]]Median' $TEMP_FILE | cut -d " " -f 2`
	lat99=`grep -e 'reads\][[:space:]]99th' $TEMP_FILE | cut -d " " -f 2`
	latAverage=`grep -e 'reads\][[:space:]]Average' $TEMP_FILE | cut -d " " -f 2`
	echo "$size $latMedian $lat99 $latAverage $throughput" >> $READ_FILE
	echo "Done read - Value size - $size"
done


######### GENERATE GRAPHS ########
cat > $GNUPLOT_SCRIPT << End-of-Message
#!`which gnuplot`
reset
set terminal png size 1024, 1024
set multiplot
set log x
set grid
set size 1,0.25
set xtics (1024, 2048, 4096, 8192, 16384, 32768, 65536)
set origin 0.0,0.75
set title "Throughput (ops/sec) Vs Size (bytes)"
plot '$WRITE_FILE' using 1:5 title "Writes" with linespoints, '$READ_FILE' using 1:5 title "Reads" with linespoints
set origin 0.0,0.50
set title "Avg Latency (ms) Vs Size (bytes)"
plot '$WRITE_FILE' using 1:4 title "Writes" with linespoints, '$READ_FILE' using 1:4 title "Reads" with linespoints
set origin 0.0,0.25
set title "99th percentile latency (ms) Vs Size (bytes)"
plot '$WRITE_FILE' using 1:3 title "Writes" with linespoints, '$READ_FILE' using 1:3 title "Reads" with linespoints
set origin 0.0,0.0
set title "Median (ms) Vs Size (bytes)"
plot '$WRITE_FILE' using 1:2 title "Writes" with linespoints, '$READ_FILE' using 1:2 title "Reads" with linespoints
unset multiplot
End-of-Message

chmod +x $GNUPLOT_SCRIPT 
./$GNUPLOT_SCRIPT > $LAT_SIZE_IMG_FILE
rm -f $GNUPLOT_SCRIPT $WRITE_FILE $READ_FILE $TEMP_FILE
