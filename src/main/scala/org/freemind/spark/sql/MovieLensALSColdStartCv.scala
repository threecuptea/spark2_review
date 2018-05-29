package org.freemind.spark.sql

import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.recommendation.ALS
import org.apache.spark.ml.tuning.{CrossValidator, CrossValidatorModel, ParamGridBuilder}
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.functions.{desc, explode, lit}

/**
  * CrossValidator is for ML tuning.  However, it needs some work to get the best parameter map used by the
  * CrossValidatorModel
  *
  * @author sling/ threecuptea rewrite, consolidate common methods into MovieLensCommon and clean-up 05/27/2018
  */
object MovieLensALSColdStartCv {

  def main(args: Array[String]): Unit = {
    if (args.length < 3) {
      println("Usage: MovieLensALSColdStartCv [movie_ratings] [personal_ratings] [movies]")
      System.exit(-1)
    }

    val mrFile = args(0)
    val prFile = args(1)
    val movieFile = args(2)

    val spark = SparkSession.builder().appName("MovieLensALSColdStartCv").config("spark.sql.shuffle.partitions", 8).
      //config("spark.sql.crossJoin.enabled", "true")
      getOrCreate()
    import spark.implicits._

    val mlCommon = new MovieLensCommon

    val mrDS = spark.read.textFile(mrFile).map(mlCommon.parseRating).cache()
    val prDS = spark.read.textFile(prFile).map(mlCommon.parseRating).cache()
    val movieDS = spark.read.textFile(movieFile).map(mlCommon.parseMovie).cache()

    mrDS.show(10, false)
    println(s"Rating Counts: movie - ${mrDS.count}, personal - ${prDS.count}")
    movieDS.show(10, false)
    println(s"Movie Counts: ${movieDS.count}")
    println()

    val allDS = mrDS.union(prDS)

    //Need to match field names of rating, KEY POINT is coldStartStrategy = "drop": drop lines with 'prediction' = 'NaN'
    val als = new ALS().setMaxIter(20).setUserCol("userId").setItemCol("movieId").setRatingCol("rating").setColdStartStrategy("drop")

    val bestModelFromCv = mlCommon.getBestCrossValidatorModel(als, mrDS)

    //Array[ParamMap] zip Array[Double], get bestParamMap so that I can refit with allDS Dataset, sorted no need of augument, sortBy (one argument)
    //sortWith (two arguments comparison, sortby reverse + (Ordering[Double].reverse), you can use - in sortBy, sortWith(_._2 > _._2)
    val bestParamsFromCv = (bestModelFromCv.getEstimatorParamMaps zip bestModelFromCv.avgMetrics).minBy(_._2)._1
    println(s"The best model from CossValidator was trained with param = ${bestParamsFromCv}")

    val augModelFromCv = als.fit(allDS, bestParamsFromCv)
    ///recommendation: org.apache.spark.sql.Dataset[org.apache.spark.sql.Row] = [userId: int, recommendations: array<struct<movieId:int,rating:float>>]
    //We explode to flat array then retrieve field from a struct
    val recommendDS = augModelFromCv.recommendForAllUsers(20).
      select($"userId", explode($"recommendations").as("recommend")).
      select($"userId", $"recommend".getField("movieId").as("movieId"), $"recommend".getField("rating").as("rating"))

    val pUserId = 0
    println(s"The top recommendation on AllUsers filter with  user ${pUserId} from ALS model from CV")
    recommendDS.filter($"userId" === pUserId).join(movieDS, recommendDS("movieId") === movieDS("id"), "inner").
      select($"movieId", $"title", $"genres", $"userId", $"rating").show(false)

    val identifier = System.currentTimeMillis()
    bestModelFromCv.save(s"output/cv-model-${identifier}")

    val loadedCvModel = CrossValidatorModel.load(s"output/cv-model-${identifier}")
    assert(loadedCvModel != null)

    recommendDS.write.option("header", true).parquet(s"output/recommendation-${identifier}")

    spark.stop()
  }

}
