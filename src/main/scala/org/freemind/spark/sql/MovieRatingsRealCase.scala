package org.freemind.spark.sql

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window


/**
  * export SPARK_HOME=[your-spark-path]
  *
  * $SPARK_HOME/bin/spark-submit --master local[4] --class org.freemind.spark.sql.MovieRatingsRealCase target/scala-2.11/spark2_review_2.11-0.1.jar \
  * data/movies_metadata data/ratings/*/partition*/*.csv.gz
  *
  * Please add output directory 'output'
  *
  * @author sling/ threecuptea 08/11, source one interview question
  */
object MovieRatingsRealCase {

  //java String[] arags
  def main(args: Array[String]): Unit = {

    if (args.length < 2) {
      println("MovieRatingsRealCase [movie-meta path] [ratings paths] ")
      System.exit(-1)
    }

    val spark = SparkSession.builder().appName("MovieRatingsRealCase").config("spark.sql.shuffle.partitions", 1).getOrCreate()
    import spark.implicits._

    val moviePath = args(0)
    val ratingPaths = args.slice(1, args.length)

    //Nullable
    val schemaRating = StructType(
        StructField("userId", IntegerType, true) ::
        StructField("movieId", IntegerType, true) ::
        StructField("rating", FloatType, true) ::
        StructField("ts", LongType, false) :: Nil
    )

    val schemaMovie = StructType(
      StructField("id", IntegerType, true) ::
        StructField("runtime", IntegerType, true) ::
        StructField("title", StringType, true) :: Nil
    )

    //It turns to var args with _*. Use distinct to de-duplicate
    val rawRatingDF = spark.read.schema(schemaRating).option("header", false).csv(ratingPaths:_*).distinct().cache()
    println(s"rating count before applying window rank: ${rawRatingDF.count()}")

    //Did not know rank is so useful.  It's since 1.4 from RDD, choose the latest
    val w = Window.partitionBy('userId, 'movieId).orderBy(desc("ts"))
    val ratingDF = rawRatingDF
      .select('userId, 'movieId, 'rating, 'ts, rank.over(w).as("rank")).filter('rank === 1).cache()
    println(s"rating count after applying window rank: ${ratingDF.count()}")

    val ratingGrouping = ratingDF.groupBy('movieId).agg(avg('rating).as("avg_rating"), count('userId).as("num_votes")).cache()

    spark.read.schema(schemaMovie).option("header", true).option("quote","\"").option("escape","\"").csv(moviePath)
      .join(ratingGrouping, 'id === ratingGrouping("movieId"), "left_outer").select('id, 'runtime, 'title, 'avg_rating, 'num_votes)
        .write.option("header", true).parquet(s"output/ratings-${System.currentTimeMillis()}")

    spark.stop()
  }

}
