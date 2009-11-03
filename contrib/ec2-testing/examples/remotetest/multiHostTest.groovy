def inputFile = new java.io.File(args[0])
def hostsFile = new java.io.File("hostsGraphingData.dat2")
Map hostNameWritesMap = [:]

// Written by Matthew Garcia
/* Takes one raw data input file as an argument. Parses through the
 file and seperates the write data output by hosts used in the users test.
 It then puts the different hosts used into a file used to hold the hostnames.
 The program prints out every tenth iteration from the test of each host in a 
 way that gnuplot can print out a graph from a .pg file */

// Gets the raw data file given by arguments
if (!inputFile.exists()) {
	println "File you entered - ${inputFile.getAbsolutePath()} - does not exist."
        System.exit(1)
}

def numIterations = 0

// Parses each line in the files and splits them, placing the split data into a String array.
inputFile.getText().eachLine {
	def data = it.split(" ")
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

// Deletes file for made for hosts if it already exists, then appends the host list to the file.
hostsFile.delete()
for (hostName in hostNameWritesMap.keySet())
	hostsFile.append(hostName + System.getProperty('line.separator'))

// Gets an int value of 10% of the total number of iterations.
def percent = numIterations / 10
BigDecimal bd = new BigDecimal(Double.toString(percent));
bd = bd.setScale(0, BigDecimal.ROUND_HALF_UP);

println numIterations

for (i in 0..numIterations - 1) {
	// Prints the data if the iteration number is divisible by 10% of the total number of iterations
	if ((i + 1) % percent == 0) {
		print "${(i + 1)} "
		
		// Prints the write record for each host. and each tenth iteration, then creates a new line.
		for (hostName in hostNameWritesMap.keySet())
			print "${hostNameWritesMap[hostName][i]} "
	
		println ""
	}
}
