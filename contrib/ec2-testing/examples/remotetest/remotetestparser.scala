#!/bin/sh
exec scala "$0" "$@"
!#

val ITERATOR_LINE_TAG = " iteration = "
val ITERATIONS_TOTAL_LINE_TAG = "iterations : "
val THROUGHPUT_LINE_TAG = "Throughput: "

class Iteration {

  var reads = 0.0

  var writes = 0.0

  var deletes = 0.0

}

var currentIterationMap = scala.collection.mutable.Map[String, Int]()
val hostNamesMap = scala.collection.mutable.Map[String, scala.collection.mutable.Map[Int, Iteration]]()

if (args.length > 0) {
  for (line <- scala.io.Source.fromFile(args(0)).getLines) {
    val hostName = line.split(":")(0)
    var iterationMap = hostNamesMap.getOrElseUpdate(hostName, scala.collection.mutable.Map[Int, Iteration]())

    var i = line.indexOf(ITERATOR_LINE_TAG)

    if (i != -1) {
      var iterationIndex = java.lang.Integer.parseInt(line.substring(i + ITERATOR_LINE_TAG.length()).replace("=", "").trim())
      currentIterationMap.put(hostName, iterationIndex)
    } else {
      i = line.indexOf(THROUGHPUT_LINE_TAG)

      if (i != -1) {
        var iterationIndex = currentIterationMap.getOrElseUpdate(hostName, 0)
        var iteration = iterationMap.getOrElseUpdate(iterationIndex, new Iteration)
        val value = java.lang.Double.parseDouble(line.substring(i + THROUGHPUT_LINE_TAG.length(), line.indexOf(" ", i + THROUGHPUT_LINE_TAG.length())))

        if (line.contains("reads"))
          iteration.reads = value
        else if (line.contains("writes"))
          iteration.writes = value
        else if (line.contains("deletes"))
          iteration.deletes = value
      }
    }
  }
}

hostNamesMap.foreach(arg => {
    val hostName = arg._1
    val iterationMap = arg._2
    val iterationList:List[Int] = List.fromIterator(iterationMap.keys).sort(_ < _)

    iterationList.foreach(iterationIndex => {
        val iterationOption:Option[Iteration] = iterationMap.get(iterationIndex)
        val iteration:Iteration = iterationOption.getOrElse(new Iteration)
        println(hostName + " " + iterationIndex + " " + iteration.reads + " " + iteration.writes + " " + iteration.deletes)
    })
  }
)
