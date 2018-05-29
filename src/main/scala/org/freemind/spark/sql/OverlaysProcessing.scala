package org.freemind.spark.sql

import com.mongodb.spark.MongoSpark
import com.mongodb.spark.config.WriteConfig
import com.tivo.unified.IdGen.gen
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{lit, udf, when}


/**
  * This parses Tivo's show content/ collection specification CSV file
  * 1. To categorize using SHOWTYPE etc. criteria
  * 2. Call java static method (IdGen class) & get sbt java/ scala build working and put together a udf type to
  * generate overlay numeric value for content/ collection/ station ids.
  * 3. Save key values to Mongodb  OverlaysLookup collection of unified database so that I can create IdAuthResponse simulator.
  *
  * $SPARK_HOME/bin/spark-submit --master local[4] --packages org.mongodb.spark:mongo-spark-connector_2.11:2.2.2 \
  * --class org.freemind.spark.sql.OverlaysProcessing target/scala-2.11/spark_tutorial_2_2.11-1.0.jar data/content_overlays
  *
  * @author sling/ threecuptea 05/26/2018
  */
object OverlaysProcessing {

  def computeGen(identifier: String, key: Int): Long = gen(identifier, key)

  def main(args: Array[String]): Unit = {

    if (args.length < 2) {
      println("Usage: ContentOverlays [movie_ratings] [content-overlays-path] [station-overlays-path]")
      System.exit(-1)
    }
    val contentPath = args(0)
    val stationPath = args(1)

    val spark = SparkSession.builder().appName("overlays-processing").config("spark.sql.shuffle.partitions", 1).
      getOrCreate()
    import spark.implicits._

    val genUdf = udf(computeGen(_: String, _: Int): Long)

    /**
     * content overlays
     */
    val contentDf = spark.read.option("header", true).option("inferSchema", true).csv(contentPath)

    //transform resource_type, mfsid, coll_tmsid, ct_numeric, cl_numeric,
    val contentOverlays = contentDf.withColumn("resource_type", when($"SHOWTYPE" === 8, "movie_overlay").
      when($"SHOWTYPE" === 3, "other_overlay").
      when($"SHOWTYPE" === 5, when($"TMSID".startsWith("EP"), "episode_overlay").otherwise("series_overlay"))).
      withColumn("coll_tmsid", $"SERIESTMSID".substr(3, 10).cast("int")).
      withColumn("ct_numeric", genUdf(lit("ct"), $"mfsid")).
      withColumn("cl_numeric", genUdf(lit("cl"), $"coll_tmsid")).
      withColumnRenamed("sourceProgramId", "rovi_id").
      withColumnRenamed("TITLE", "title")

    println("Content Overlays counts")
    contentOverlays.groupBy($"resource_type").count().show(false)
    println()

    println("Movie Samples:")
    contentOverlays.select($"rovi_id", $"resource_type", $"mfsid", $"ct_numeric", $"coll_tmsid", $"cl_numeric",
      $"title").where($"resource_type" === "movie_overlay").show(false)

    /**
      * station overlays
      */
    val stationDf = spark.read.option("header", true).option("inferSchema", true).csv(stationPath)

    //sourceStationId, mfsid
    val stationOverlays = stationDf.withColumn("resource_type", lit("station_overlay")).
      withColumn("st_numeric", genUdf(lit("st"), $"mfsid")).
      withColumnRenamed("sourceStationId", "rovi_id")

    println(s"Station Overlays counts: ${stationOverlays.count}")

    val writeConfig = WriteConfig(Map("uri" -> "mongodb://localhost:27017/unified.OverlaysLookup2") )

    val start = System.currentTimeMillis()

    MongoSpark.save(contentOverlays.select($"rovi_id", $"resource_type", $"mfsid", $"ct_numeric", $"coll_tmsid", $"cl_numeric",
      $"title").write.mode("overwrite"), writeConfig)

    MongoSpark.save(stationOverlays.select($"rovi_id", $"resource_type", $"mfsid", $"st_numeric").write.mode("append"), writeConfig)

    printf("Execution time= %7.3f seconds\n", (System.currentTimeMillis() - start)/1000.00)

    spark.stop()
  }

}
