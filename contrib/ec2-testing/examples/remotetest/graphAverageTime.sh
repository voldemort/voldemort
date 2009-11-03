# Written by Matthew Garcia
# Runs a groovy script that outputs data to a .dat file.
# Creates a .pg file based on the datafile given by the user.
# The .pg file is run using the .dat file created, and output 
# directed to a .png file, creating a graph for the user.
# After the program is done with the .dat file and .pg file
# it then deletes the files.

DATE='date+%C%y%-%m-%d_%H_%M_%S'
PG_FILE=${DATE}graphAverageTime.pg
IMG_FILE=${DATE}graphAverageTime.png
DATA_FILE=${DATE}graphAverageTime.dat

if [ "$1" = "" ]
        then
        echo "Usage: $0 Takes raw data files as arguments."
        exit 1
fi

groovy graphAverageTime.groovy $@ > $DATA_FILE
cat > $PG_FILE <<End-of-Message 
#!/usr/bin/gnuplot" 
reset 
set terminal png
set xlabel "Test Number"
set ylabel "Transactions/Second"
set yrange [0:10000]
set title "Average Transactions per second"
set key reverse Left outside
set grid
set style data linespoints
plot '$datafile' using 1:2 title "Reads", \\
"" using 1:3 title "Writes", \\
"" using 1:4 title "Deletes"
End-of-Message
chmod +x $PG_FILE
./$PG_FILE > $IMG_FILE
rm -f $PG_FILE $DATA_FILE
