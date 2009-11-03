## Written by Matthew Garcia
# Runs a groovy script that creates a text file containing the information
# that will be parsed, as well as a text file containing the hostnames of 
# the machines used in the test. It uses awk to get the number of hosts
# in the script. Afterwards it creates a .pg file using the hosts file 
# and the text file containing the information to be graphed. After 
# creating the .pg file it is run and output is directed to a .png file.
# The .png file contains the data created from the groovy script in a graph.

DATE='date+%C%y%-%m-%d_%H_%M_%S'
PG_FILE=${DATE}$PG_FILE
IMG_FILE=${DATE}$IMG_FILE
DATA_FILE=${DATE}multiHostTest.dat

if [ "$1" = "" ]
	then
	echo "Usage: $0 Takes raw data files as arguments."
	exit 1
fi

groovy multiHostTest.groovy $@ > $DATA_FILE

intI=`awk 'END{print NR}' hostsGraphingData.dat2`

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
	# If the current host is the first host, it appends a plot String to the .pg file.
	if [ $i == 1 ] 
		then
			echo "plot \"$datafile\" using 1:$(($i + 1)) title \"$(awk -v i=$i 'NR==i {print $1}' hostsGraphingData.dat2)\", \\" >> $PG_FILE
	# If the current host is the last host, it appends a string starting with "" (which means using the same data stated already) and ends without a \
	elif [ $i == $intI ]
		then
			echo "\"\" using 1:$(($i + 1)) title \"$(awk -v i=$i 'NR==i {print $1}' hostsGraphingData.dat2)\"" >> $PG_FILE
	# If neither of the above cases are true the file will append to the file the "" string with the ending having a , and \ (meaning that the file 
	# has more to plot.
	else 
		echo "\"\" using 1:$(($i + 1)) title \"$(awk -v i=$i 'NR==i {print $1}' hostsGraphingData.dat2)\", \\" >> $PG_FILE	
	fi
}

chmod +x $PG_FILE
./$PG_FILE > $IMG_FILE
rm -f $DATA_FILE hostsGraphingData.dat2 $PG_FILE
