// Written by Matthew Garcia
/* Takes one raw data input file as an argument. Parses through the
 file and seperates the write data output by hosts used in the users test.
 It then puts the different hosts used into a file used to hold the hostnames.
 The program prints out every tenth iteration from the test of each host in a 
 way that gnuplot can print out a graph from a .pg file */

def inputFile = new java.io.File(args[0])
Map hostNameWritesMap = [:]

// Gets the raw data file given by arguments
if (!inputFile.exists()) {
	println "File you entered - ${inputFile.getAbsolutePath()} - does not exist."
        System.exit(1)
}

def numIterations = 0

// Parses each line in the files and splits them, placing the split data into a String array.
inputFile.getText().eachLine {
	def data = it.split(" ")

	if (data.length >= 4) {
		def host = data[0]
		def writes = data[3]

		// If the user is using an instance of ec2, this cuts off the tail end of the host name.
		def hostName = host.replace(".compute-1.amazonaws.com", "")
		def writesList = hostNameWritesMap[hostName]

		if (writesList == null) {
			writesList = []
			hostNameWritesMap[hostName] = writesList
		}
	
		// Adds the write record to a list containing all writes in the file(mapped to a particular hostname).
		writesList << writes
		numIterations = Math.max(numIterations, writesList.size())
	}
}

// Gets an int value of 10% of the total number of iterations.
//def percent = numIterations / 10

println numIterations

for (i in 0..numIterations - 1) {
	// Prints the data if the iteration number is divisible by 10% of the total number of iterations
	//if ((i + 1).intValue() % percent == 0) {
		print "${(i + 1)} "
		
		// Prints the write record for each host. and each tenth iteration, then creates a new line.
		for (hostName in hostNameWritesMap.keySet())
			print "${hostNameWritesMap[hostName][i]} "
	
		println ""
	//}
}
