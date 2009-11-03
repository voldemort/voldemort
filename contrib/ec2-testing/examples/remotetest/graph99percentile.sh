# Written by Matthew Garcia
# Creates a .pg file based on the datafile given by the user.
# The program then runs the .pg file and outputs the data to
# a .png file, creating a graph for the user.
# After the program is done with the data file and .pg file
# it then deletes the files.

DATE=`date +%C%y_%m_%d_%H_%M_%S`
PG_FILE=${DATE}graph99percentile.pg
IMG_FILE=graph99percentile.png
DATA_FILE=${DATE}graph99percentile.dat

if [ "$1" = "" ]
        then
        echo "Usage: $0 Takes raw data files as arguments."
        exit 1
fi

groovy graph99percentile.groovy $@ > $DATA_FILE

# Creates a .pg file by outputting the file strings to it.

cat > $PG_FILE <<End-of-Message
#!/usr/bin/gnuplot
reset
set terminal png
set xlabel "Test Number"
set ylabel "Transactions/Second"
set yrange [0:10000]
set title "99 percent of Transactions per second"
set key reverse Left outside
set grid
set style data linespoints
plot "${DATA_FILE}" using 1:2 title "Reads", \\
"" using 1:3 title "Writes", \\
"" using 1:4 title "Deletes"
End-of-Message

chmod +x $PG_FILE
./$PG_FILE > $IMG_FILE
rm -f $PG_FILE $DATA_FILE
