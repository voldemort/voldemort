#!/bin/sh
# Written by Matthew Garcia
# Runs a groovy script that creates a text file containing the information
# that will be parsed, as well as a text file containing the hostnames of 
# the machines used in the test. It uses awk to get the number of hosts
# in the script. Afterwards it creates a .pg file using the hosts file 
# and the text file containing the information to be graphed. After 
# creating the .pg file it is run and output is directed to a .png file.
# The .png file contains the data created from the groovy script in a graph.

DATE=`date +%C%y_%m_%d_%H_%M_%S`
PG_FILE=${DATE}graphMultiHostTest.pg
IMG_FILE=graphMultiHostTest.png
DATA_FILE=${DATE}graphMultiHostTest.dat
HOSTS_FILE=${DATE}graphMultiHostTest-hosts.dat

if [ "$1" = "" ]
	then
		echo "Usage: $0 Takes raw data files as arguments."
		exit 1
fi

./graphMultiHostTest.scala $@ > $DATA_FILE

cat $1  | cut -d' ' -f1 | sort | uniq > $HOSTS_FILE
intI=`awk 'END{print NR}' $HOSTS_FILE`

# Creates the .pg file
cat > $PG_FILE <<End-of-Message
#!/usr/bin/gnuplot
reset
set terminal png
set xlabel "Test Number"
set ylabel "Transactions/Second"
set yrange [0:8000]
set title "Average Transactions per second"
set key reverse Left outside
set grid
set style data linespoints
End-of-Message

# Loops through and appends to the .pg file for each host.
for ((i = 1; $i <= $intI; i++)) {
	hostName=$(awk -v i=$i 'NR==i {print $1}' $HOSTS_FILE)
	usingLine="using 1:$(($i + 1)) title \"$hostName\""

	if [ $i == 1 ] 
		then
			# If the current host is the first host, it appends a 
			# plot String to the .pg file.
			echo -n "plot \"$DATA_FILE\" $usingLine" >> $PG_FILE
		else 
			# If neither of the above cases are true the file will
			# append to the file the "" string with the ending
			# having a , and \ (meaning that the file has more to 
			# plot.
			echo -n "\"\" $usingLine" >> $PG_FILE	
	fi

	if [ $i != $intI ]
		then
			# If the current host is NOT the last host, it appends
			# a string starting with "" (which means using the same
			# data stated already) and ends without a \
			echo ", \\" >> $PG_FILE
		else
			# This is just to be nice formatting since we don't
			# put out any newlines above
			echo "" >> $PG_FILE
	fi
}

chmod +x $PG_FILE
./$PG_FILE > $IMG_FILE
rm -f $DATA_FILE $HOSTS_FILE $PG_FILE
