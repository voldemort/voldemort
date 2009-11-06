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

def get99thPercentile(times:List[Double]) : Double = {
  val timesSorted = times.sort(_ < _)
  val percent = (timesSorted.length * 0.99).asInstanceOf[Int]
  timesSorted(percent)
}

var i = 0

for (arg <- args) {
  val writes = new scala.collection.mutable.ListBuffer[Double]()
  val deletes = new scala.collection.mutable.ListBuffer[Double]()
  val reads = new scala.collection.mutable.ListBuffer[Double]()

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
