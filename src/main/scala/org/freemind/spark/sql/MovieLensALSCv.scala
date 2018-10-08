package org.freemind.spark.sql

import org.apache.spark.ml.recommendation.ALS
import org.apache.spark.ml.tuning.CrossValidatorModel
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.explode

/**
  * CrossValidator is for ML tuning.  However, it needs some work to get the best parameter map used by the
  * CrossValidatorModel
  *
  * @author sling/ threecuptea consolidated common methods into MovieLensCommon and refactored on 05/27/2018
  */
object MovieLensALSCv {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName("MovieLensALSCv").getOrCreate()
    import spark.implicits._

    val mlCommon = new MovieLensCommon(spark)
    val (mrDS, prDS, movieDS) = mlCommon.getMovieLensDataset()

    mrDS.show(10, false)
    println(s"Rating Counts: movie - ${mrDS.count}, personal - ${prDS.count}")
    movieDS.show(10, false)
    println(s"Movie Counts: ${movieDS.count}")
    println()

    val allDS = mrDS.union(prDS)

    //Need to match field names of rating, KEY POINT is coldStartStrategy = "drop": drop lines with 'prediction' = 'NaN'
    val als = new ALS().setMaxIter(20).setUserCol("userId").setItemCol("movieId").setRatingCol("rating").setColdStartStrategy("drop")

    val bestModelFromCv = mlCommon.getBestCrossValidatorModel(als, mrDS)

    //Array[ParamMap] zip Array[Double], get bestParamMap so that I can refit with allDS Dataset, sorted requires no augument, sortBy requires one argument)
    //sortWith requires two arguments comparison, sortby reverse + (Ordering[Double].reverse), you can use _ in sortBy, sortWith(_._2 > _._2)
    val bestParamsFromCv = (bestModelFromCv.getEstimatorParamMaps zip bestModelFromCv.avgMetrics).minBy(_._2)._1
    println(s"The best model from CossValidator was trained with param = ${bestParamsFromCv}")

    val augModelFromCv = als.fit(allDS, bestParamsFromCv)
    ///recommendation: org.apache.spark.sql.Dataset[org.apache.spark.sql.Row] = [userId: int, recommendations: array<struct<movieId:int,rating:float>>]
    //We explode to flat array then retrieve field from a struct
    val recommendDS = augModelFromCv.recommendForAllUsers(20).
      select($"userId", explode($"recommendations").as("recommend")).
      select($"userId", $"recommend".getField("movieId").as("movieId"), $"recommend".getField("rating").as("rating"))

    val pUserId = 0
    println(s"The top recommendation on AllUsers filter with  user ${pUserId} from CV")
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