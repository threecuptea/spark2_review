package org.freemind.spark.sql

import com.mongodb.spark.MongoSpark
import com.mongodb.spark.config.{ReadConfig, WriteConfig}
import org.apache.spark.ml.recommendation.ALS
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DoubleType

/**
  * Inspired by http://cdn2.hubspot.net/hubfs/438089/notebooks/MongoDB_guest_blog/Using_MongoDB_Connector_for_Spark.html
  *
  * Using convert_csv.py to convert "::" delimiter to "," then
  * mongoimport -d movielens -c movie_ratings --type csv -f userId,movieId,rating,timestamp data/ratings.csv
  *
  * To run it locally
  * $SPARK_HOME/bin/spark-submit --packages org.mongodb.spark:mongo-spark-connector_2.11:2.2.2 \
  * --master local[*] --class org.freemind.spark.sql.MovieLensALSMongo target/scala-2.11/spark_tutorial_2_2.11-1.0.jar
  *
  * @author sling(threecuptea) wrote on 12/30/2016 - 2/4/2017 .
  */
object MovieLensALSMongo {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName("MovieLensALSMongo").config("spark.sql.shuffle.partitions", 8).
      //config("spark.sql.crossJoin.enabled", "true")
      getOrCreate()
    import spark.implicits._

    val mrReadConfig = ReadConfig(Map("uri" -> "mongodb://localhost:27017/movielens.movie_ratings?readPreference=primaryPreferred"))
    val prReadConfig = ReadConfig(Map("uri" -> "mongodb://localhost:27017/movielens.personal_ratings?readPreference=primaryPreferred"))
    val movieReadConfig = ReadConfig(Map("uri" -> "mongodb://localhost:27017/movielens.movies?readPreference=primaryPreferred"))
    val writeConfig = WriteConfig(Map("uri" -> "mongodb://localhost:27017/movielens.recommendations"))
    //MongoSpark.toDS route does not work
    //val mrDS = MongoSpark.load(spark, mrReadConfig, classOf[MongoRating]) //Will get error 'Cannot infer type for class org.freemind.spark.sql.MongoRating because it is not bean-compliant'
    //I have to use old route toDF then as to DS.  MongoSpark.load(spark, mrReadConfig) return DataFrame then map to type
    // MongoSpark.load(spark, mrReadConfig) return RDD
    val mrDS = MongoSpark.load(spark, mrReadConfig).map(r => Rating(r.getAs[Int]("userId"), r.getAs[Int]("movieId"), r.getAs[Int]("rating"))).cache()
    val prDS = MongoSpark.load(spark, prReadConfig).map(r => Rating(r.getAs[Int]("userId"), r.getAs[Int]("movieId"), r.getAs[Int]("rating"))).cache()
    val movieDS = MongoSpark.load(spark, movieReadConfig).map(r => Movie(r.getAs[Int]("id"), r.getAs[String]("title"), r.getAs[String]("genres").split('|'))).cache()

    mrDS.show(10, false)
    println(s"Rating Counts: movie - ${mrDS.count}, personal - ${prDS.count}")
    movieDS.show(10, false)
    println(s"Movie Counts: ${movieDS.count}")
    println()

    val allDS = mrDS.union(prDS)

    val mlCommon = new MovieLensCommon

    //Need to match field names of rating, KEY POINT is coldStartStrategy = "drop": drop lines with 'prediction' = 'NaN'
    val als = new ALS().setMaxIter(20).setUserCol("userId").setItemCol("movieId").setRatingCol("rating").setColdStartStrategy("drop")

    val bestParmsFromALS = mlCommon.getBestParmMapFromALS(als, mrDS)
    println(s"The best model from ALS was trained with param = ${bestParmsFromALS}")

    val augModelFromALS = als.fit(allDS, bestParmsFromALS)

    val recommendDS = augModelFromALS.recommendForAllUsers(20).
      select($"userId", explode($"recommendations").as("recommend")).
      select($"userId", $"recommend".getField("movieId").as("movieId"), $"recommend".getField("rating").as("rating"))

    MongoSpark.save(recommendDS.select($"userId", $"movieId", $"rating".cast(DoubleType)).
      write.mode("overwrite"), writeConfig)

    spark.stop()
  }

}
