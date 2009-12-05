#!/bin/sh
exec scala "$0" "$@"
!#

// Written by Matthew Garcia
/* Takes one raw data input file as an argument. Parses through the
 file and seperates the write data output by hosts used in the users test.
 It then puts the different hosts used into a file used to hold the hostnames.
 The program prints out every tenth iteration from the test of each host in a 
 way that gnuplot can print out a graph from a .pg file */

val hostNameWritesMap = scala.collection.mutable.Map[String, scala.collection.mutable.ListBuffer[String]]() 
var numIterations = 0
val hostNames = new java.util.TreeSet[String]()

if (args.length > 0) {
  for (line <- scala.io.Source.fromFile(args(0)).getLines) {
    val data = line.split(" ")

    if (data.length >= 4) {
      val hostName = data(0)
      val writes = data(3)
      var writesList = hostNameWritesMap.getOrElseUpdate(hostName, new scala.collection.mutable.ListBuffer[String]())
	
      // Adds the write record to a list containing all writes in the file(mapped to a particular hostname).
      writesList.append(writes)
      numIterations = Math.max(numIterations, writesList.length)
      hostNames.add(hostName)
    }
  }
}

// Gets an int value of 10% of the total number of iterations.
val percent = numIterations / 10
val hostNameList:List[String] = List.fromArray(hostNames.toArray()).asInstanceOf[List[String]]
		
for (i <- 0 until numIterations) {
  // Prints the data if the iteration number is divisible by 10% of the total number of iterations
  if (i % percent == 0) {
    print((i + 1) + " ")
		
    // Prints the write record for each host. and the Nth iteration, then creates a new line.
    for (hostName <- hostNameList) {		
      var writesList = hostNameWritesMap.getOrElseUpdate(hostName, new scala.collection.mutable.ListBuffer[String]())
      var value = writesList(i)

      print(value + " ")
    }	

    println("")
  }
}
