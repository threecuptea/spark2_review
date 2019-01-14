package org.freemind.spark.sql

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._


/*
* @author sling/ threecuptea 05/26/2018
 */
object ResolverDetailsJenga {

  def main(args: Array[String]): Unit = {

    if (args.length < 5) {
      println("Usage: ResolverDetailsJenga [non-jt-log] [jt-log] [jenga-lower] [jenga-upper] [reource-1] [resource_2]....")
      System.exit(-1)
    }

    val nonJtLog = args(0)
    val jtLog = args(1)
    val lower = args(2).toLong
    val upper = args(3).toLong
    val resources = args.slice(4, args.length)

    println(s"Request resources: ${resources.mkString(",")}")

    val parser = new DanubeLogParser(resources: _*)

    val spark = SparkSession.builder().appName("ResolverDetailsJenga").config("spark.sql.shuffle.partitions", 1).
      getOrCreate()
    import spark.implicits._

    val nonJtDs = spark.read.textFile(nonJtLog).flatMap(parser.parseResolverRaw(_, false)).filter('pubId.between(lower, upper))
        .withColumnRenamed("dirty_size", "non_jt_dirty_size").cache()

    val jtDs = spark.read.textFile(jtLog).flatMap(parser.parseResolverRaw(_, true)).filter('pubId.between(lower, upper))
      .withColumnRenamed("dirty_size", "jt_dirty_size").cache()

    for (res <- resources) {
      val joinedDS = nonJtDs.filter('resource === res).join(jtDs.filter('resource === res),
        Seq("pubId", "resource", "roviId", "old_pubId"), "inner")

      joinedDS.withColumn("diff", 'jt_dirty_size - 'non_jt_dirty_size)
          .withColumn("difference", format_string("%-,8d", 'diff))
          .withColumn("abs_diff", abs('diff))
          .sort(desc("abs_diff"))
          .select('resource, 'roviId, 'pubId, 'old_pubId, 'non_jt_dirty_size, 'jt_dirty_size, 'difference)
          .show(50, truncate=false)
    }

    spark.stop()
  }

}
