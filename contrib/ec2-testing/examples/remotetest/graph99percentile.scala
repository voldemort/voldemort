#!/bin/sh
exec scala "$0" "$@"
!#

// Written by Matthew Garcia
/* Program takes the raw data files provided by arguments and
 then parses each file adding the writes, reads, and deletes
 transactions to seperate Lists. After this it sorts the lists
 by value and gets the index that is 99% of the total size of the
 list. It then prints each test number followed by the 99% highest
 of read, write, and delete transactions(in a format that gnuplot can
 use to create a graph based on the data). Output may be changed so 
 that this information is sent to a file. */

import scala.collection.mutable.ListBuffer
	
def get99thPercentile(times:List[Double]) : Double = {
  val timesSorted = times.sort(_ < _)
  val percent = timesSorted.length * 0.99
  var bd = new BigDecimal(new java.math.BigDecimal(percent))
  bd = bd.setScale(0, BigDecimal.RoundingMode.ROUND_HALF_UP)
  timesSorted(bd.intValue() - 1)
}

if (args.length <= 0) {
  System.err.println("Please enter at least one file")
  System.exit(1)
}

var i = 0

// Get raw data files from user.
for (arg <- args) {
  val writes = new ListBuffer[Double]()
  val deletes = new ListBuffer[Double]()
  val reads = new ListBuffer[Double]()

  for (line <- scala.io.Source.fromFile(arg).getLines) {
    val data = line.split(" ")
		
    if (data.length >= 5) {
      reads.append(data(2).toDouble)
      writes.append(data(3).toDouble)
      deletes.append(data(4).toDouble)
    }
  }

  i += 1
  val readsValue = get99thPercentile(reads.toList)
  val writesValue = get99thPercentile(writes.toList)
  val deletesValue = get99thPercentile(deletes.toList)

  println(i + " " + readsValue + " " + writesValue + " " + deletesValue)
}
