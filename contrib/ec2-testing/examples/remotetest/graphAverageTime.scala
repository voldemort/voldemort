#!/bin/sh
exec scala "$0" "$@"
!#

// Written by Matthew Garcia
/* Parses through multiple files containing raw data.
 As it parses through it adds the total time for 
 writes, deletes, and reads, and then divides them
 by the number of iterations to come up with the average.
 The program then prints out a number defining the test,
 followed by the average reads, writes, and deletes for
 that test (in a way so that gnuplot can parse and graph it). */

var i = 0

for (arg <- args) {
  var writesTotal = 0.0
  var writesCount = 0
  var deletesTotal = 0.0
  var deletesCount = 0
  var readsTotal = 0.0
  var readsCount = 0

  for (line <- scala.io.Source.fromFile(arg).getLines) {
    val data = line.split(" ")

    if (data.length >= 5) {
      // Gets total seconds for each transaction, and the total iterations.
      writesTotal += data(3).toDouble
      writesCount += 1
      deletesTotal += data(4).toDouble
      deletesCount += 1		
      readsTotal += data(2).toDouble
      readsCount += 1
    }
  }

  val writesAvg = writesTotal / writesCount
  val deletesAvg = deletesTotal / deletesCount
  val readsAvg = readsTotal / readsCount

  i += 1

  println(i + " " + readsAvg + " " + writesAvg + " " + deletesAvg)
}
