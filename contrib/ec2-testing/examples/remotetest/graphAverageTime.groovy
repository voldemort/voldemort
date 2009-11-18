def record = []
// Written by Matthew Garcia
/* Parses through multiple files containing raw data.
 As it parses through it adds the total time for 
 writes, deletes, and reads, and then divides them
 by the number of iterations to come up with the average.
 The program then prints out a number defining the test,
 followed by the average reads, writes, and deletes for
 that test (in a way so that gnuplot can parse and graph it). */

for (i in 0..args.length - 1) {
	// Get raw data files from user.
	def inputFile = new java.io.File(args[i])
	
	//Checks that the raw data files exist.
	if (!inputFile.exists()) {
    		println "File you entered - ${inputFile.getAbsolutePath()} - does not exist."
    		System.exit(1)
    	}

	def writesTotal = 0.0
	def writesCount = 0
	def deletesTotal = 0.0
	def deletesCount = 0
	def readsTotal = 0.0
	def readsCount = 0

	inputFile.getText().eachLine {
		def data = it.split(" ")
	
		if (data.length >= 5) {	
			// Gets total seconds for each transaction, and the total iterations.
			writesTotal += data[3].toDouble()
			writesCount++
			deletesTotal += data[4].toDouble()
			deletesCount++		
			readsTotal += data[2].toDouble()
			readsCount++
		}
	}	
	
	def writesAvg = writesTotal / writesCount
	def deletesAvg = deletesTotal / deletesCount
	def readsAvg = readsTotal / readsCount
		
	// Adds the transactions to a List in a way that is parsable by gnuplot.
	record.add("${i + 1} ${readsAvg} ${writesAvg} ${deletesAvg}")
} 
	
for (i in 0..record.size - 1) 
	println record[i]
