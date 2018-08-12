package org.freemind.spark.sql


import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

/*
* @author sling/ threecuptea 05/26/2018
 */
object DanubeStatesAnalysis2 {

  def main(args: Array[String]): Unit = {

    if (args.length < 6) {
      println("Usage: DanubeStatesAnalysis2 [non-jt-log] [jt-log] [non-jt-lower] [non-jt-upper] [jt-lower] [jt-upper]")
      System.exit(-1)
    }

    val nonJtLog = args(0)
    val jtLog = args(1)
    val nonJtLower = args(2).toLong
    val nonJtUpper = args(3).toLong
    val jtLower = args(4).toLong
    val jtUpper = args(5).toLong

    val spark = SparkSession.builder().appName("DanubeStatesAnalysis2").config("spark.sql.shuffle.partitions", 1).
      getOrCreate()
    import spark.implicits._

    val parser = new DanubeLogParser()
    val statesInc = Seq("PUBLISH", "UNPUBLISH")

    val nonJtDs = spark.read.textFile(nonJtLog).map(parser.parseNonJtLog2).filter('pubId.between(nonJtLower, nonJtUpper))
          .filter('state.isin(statesInc: _*)).cache()
    println(s"NON Java-transform DanubeState count = ${nonJtDs.count}")
    println()

    val jtDs = spark.read.textFile(jtLog).map(parser.parseJtLog2).filter('pubId.between(jtLower, jtUpper))
      .filter('state.isin(statesInc: _*)).cache()
    println(s"Java-transform DanubeState count = ${jtDs.count}")
    println()

    println("Union together to generate summary")
    val combinedDs = nonJtDs.union(jtDs)

    println("Count groupBy PUBLISH_STATE")
    combinedDs.groupBy($"state").agg(sum($"jtNo"), sum($"jtYes")).show(false)

    println("Count groupBy RESOURCE")
    combinedDs.groupBy($"resource").agg(sum($"jtNo"), sum($"jtYes")).show(500, false)

    println("Count groupBy RESOURCE and PUBLISH_STATE")
    combinedDs.groupBy($"resource", $"state").agg(sum($"jtNo"), sum($"jtYes")).show(500, false)

    println("Count groupBy PUBLISH_STATE and RESOURCE")
    combinedDs.groupBy('state, 'resource).agg(sum('jtNo), sum('jtYes)).show(500, false)

    spark.stop()
  }
}
