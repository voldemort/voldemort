// Written by Matthew Garcia
/* Program takes the raw data files provided by arguments and
 then parses each file adding the writes, reads, and deletes
 transactions to seperate Lists. After this it sorts the lists
 by value and gets the index that is 99% of the total size of the
 list. It then prints each test number followed by the 99% highest
 of read, write, and delete transactions(in a format that gnuplot can
 use to create a graph based on the data). Output may be changed so 
 that this information is sent to a file. */
 	
public double listSorter(List thetimes) {
	Collections.sort(thetimes)
	int size = thetimes.size()
	double percent = size * 0.99
	BigDecimal bd = new BigDecimal(Double.toString(percent))
	bd = bd.setScale(0, BigDecimal.ROUND_HALF_UP)
	return thetimes.get(bd.intValue() - 1)
}

// Get raw data files from user.
for (i in 0..args.length - 1) {
        def inputFile = new java.io.File(args[i])

	// Checks that the data files exist
        if (!inputFile.exists()) {
                println "File you entered - ${inputFile.getAbsolutePath()} - does not exist."
                System.exit(1)
        }
	
	def writes = []
	def deletes = []
	def reads = []

        inputFile.getText().eachLine {
                def data = it.split(" ")
		reads.add(data[2].toDouble())
                writes.add(data[3].toDouble())
                deletes.add(data[4].toDouble())
	}

	// Prints the data in a format that can be used by gnuplot.
	println "${i + 1} ${listSorter(reads)} ${listSorter(writes)} ${listSorter(deletes)}"
}
