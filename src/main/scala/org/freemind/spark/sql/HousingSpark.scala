package org.freemind.spark.sql

import org.apache.spark.ml.{Estimator, Pipeline}
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.feature.{Imputer, OneHotEncoder, StandardScaler, StringIndexer}
import org.apache.spark.ml.linalg.{DenseVector, Vectors, Vector}
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.regression.{DecisionTreeRegressor, LinearRegression, RandomForestRegressionModel, RandomForestRegressor}
import org.apache.spark.ml.tuning.{CrossValidator, ParamGridBuilder}
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

object HousingSpark {

  val spark = SparkSession.builder().appName("HousingSpark").getOrCreate()
  import spark.implicits._

  val evaluator = new RegressionEvaluator().setMetricName("rmse").setLabelCol("label").setPredictionCol("prediction")

  def income_cat_ratio(df: Dataset[Row], column: String) = {
    val total = df.count()
    df.groupBy("income_cat").count().withColumn(column, 'count / total).drop("count")
  }

  def getHousingSchema() = StructType(Array(
    StructField("longitude", FloatType, true),
    StructField("latitude", FloatType, true),
    StructField("housing_median_age", FloatType, true),
    StructField("total_rooms", FloatType, true),
    StructField("total_bedrooms", FloatType, true),
    StructField("population", FloatType, true),
    StructField("households", FloatType, true),
    StructField("median_income", FloatType, true),
    StructField("median_house_value", FloatType, true),
    StructField("ocean_proximity", StringType, true)
  ))

  def crossValidatorCommon(trainDF: Dataset[Row], testDF: Dataset[Row],
                           estimator: Estimator[_], pg: Array[ParamMap], label: String) = {

    val cv = new CrossValidator().setEstimator(estimator).setEstimatorParamMaps(pg).setEvaluator(evaluator).
      setNumFolds(5).setSeed(42l).setParallelism(5)  //set it the same as numFolds

    val cvModel = cv.fit(trainDF)
    val bestParam = (cvModel.getEstimatorParamMaps zip cvModel.avgMetrics).minBy(_._2)._1
    println(s"${label} Model best param map: ${bestParam}")

    val trainRmse = evaluator.evaluate(cvModel.transform(trainDF))
    val prediction = cvModel.transform(testDF)
    prediction.show(10)
    val testRmse = evaluator.evaluate(prediction)
    println(s"RMSE of ${label} Model on train, test: ${trainRmse}, ${testRmse}")
    cvModel
  }

  def crossValidatorWithLinearRegression(trainDF: Dataset[Row], testDF: Dataset[Row]) = {
    val lr = new LinearRegression().setMaxIter(10)
    //Param for the ElasticNet mixing parameter, in range [0, 1].
    //For alpha = 0, the penalty is an L2 penalty. For alpha = 1, it is an L1 penalty.
    //For regParam, 1 penalize model complexity and 0: training error accounts for all
    val pg = new ParamGridBuilder().
      addGrid(lr.regParam, Array(0.001, 0.000001, 0.000000001)).
      addGrid(lr.elasticNetParam, Array(0.01, 0.001, 0.0001)).
      build()

    crossValidatorCommon(trainDF, testDF, lr, pg, "Linear Regression")
  }

  def crossValidatorWithDecisionTree(trainDF: Dataset[Row], testDF: Dataset[Row]) = {
    val dtr =  new DecisionTreeRegressor().setMaxMemoryInMB(512).setCacheNodeIds(true)

    val pg = new ParamGridBuilder().
      addGrid(dtr.maxBins, Array(8, 16, 32)).
      addGrid(dtr.maxDepth, Array(2, 3, 5, 8)).
      addGrid(dtr.minInstancesPerNode, Array(20, 50, 100)).
      build()

    crossValidatorCommon(trainDF, testDF, dtr, pg, "Decision Tree")
  }

  def crossValidatorWithRandomForest(trainDF: Dataset[Row], testDF: Dataset[Row]) = {
    val rfr =  new RandomForestRegressor().setMaxMemoryInMB(512).setCacheNodeIds(true)
    //The default num tree = 20

    val pg = new ParamGridBuilder().
      addGrid(rfr.maxBins, Array(8, 16, 32)).
      addGrid(rfr.maxDepth, Array(5, 8, 12, 16)).
      addGrid(rfr.minInstancesPerNode, Array(20)).
      build()

    crossValidatorCommon(trainDF, testDF, rfr, pg, "Random Forest")
  }


  def main(args: Array[String]): Unit = {

    if (args.length < 1) {
      println("Usage: HousingSpark [housing csv file]....")
      System.exit(-1)
    }

    val housingPath = args(0)
    val housingSchema = getHousingSchema()

    val housing = spark.read.option("header", true).schema(housingSchema).csv(housingPath)
    /*
      Some statistics and information
     */
    println(s"Raw housing count: ${housing.count}")
    housing.printSchema()

    housing.show(10, true)
    housing.groupBy('ocean_proximity).count().orderBy(desc("count")).show(false)

    housing.describe().show(false)
    val numFieldNames  = housing.schema.fieldNames.take(9)
    val numMedians = housing.stat.approxQuantile(numFieldNames, Array(0.5), 0.001)
    // It will return Array(Array(-118.5199966430664), Array(34.27000045776367), Array(29.0),......))
    //print numeric fields and itheir median values
    val medians = numFieldNames zip numMedians.map(arr => arr(0))
    println(s"*** Medians = ${medians.mkString(", ")}")

    /*
      Sampling
     */
    val housing_income_cat = housing.withColumn("income_cat", when(ceil('median_income / 1.5) < 5, ceil('median_income / 1.5)).otherwise(5.0))

    val Array(sample_random_train, sample_random_test) = housing_income_cat.randomSplit(Array(0.8, 0.2), 42l)
    println(s"Housing_income_cat, random split= ${sample_random_train.count}, ${sample_random_test.count}")

    val fractions = housing_income_cat.select('income_cat).distinct().rdd.map {
      case Row(key: Double) => key -> 0.8
    }.collectAsMap().toMap


    val sample_strat_train = housing_income_cat.stat.sampleBy("income_cat", fractions, 42l)
    val sample_strat_test = housing_income_cat.except(sample_strat_train)
    println(s"Housing_income_cat, stratified split= ${sample_strat_train.count}, ${sample_strat_test.count}")

    val compare_ratio = income_cat_ratio(housing_income_cat, "Overall").join(
      income_cat_ratio(sample_random_train, "Random"), "income_cat").join(
      income_cat_ratio(sample_strat_train, "Stratified"), "income_cat").
      withColumn("Rand. %error", 'Random / 'Overall * 100 - 100).
      withColumn("Strat. %error", 'Stratified / 'Overall * 100 - 100).orderBy("income_cat")

    compare_ratio.show()

    /*
      Clean-up, transform and normalize data
    */
    val housing_cat = housing.select($"ocean_proximity")
    //Imputer is for numeric field(s). Here, fill in missing 'total_bedrooms' using median and output 'total_bedrooms_out'
    val imputer = new Imputer().setStrategy("median").setInputCols(Array("total_bedrooms")).setOutputCols(Array("total_bedrooms_out"))
    //StringIndexer is for categorical field.  Convert string valud to index
    val indexer = new StringIndexer().setInputCol("ocean_proximity").setOutputCol("op_index")
    val indexed = indexer.fit(housing_cat).transform(housing_cat)
    val op_cat_arr = indexed.distinct().orderBy("op_index").map(r => r.getString(0)).collect()

    //If there are 5 categories, it will generate [0.0, 0.0, 0.0, 0.0] for an input value of 4.0 maps if dropLast = true.  It save space
    val encoder = new OneHotEncoder().setInputCol("op_index").setOutputCol("ocean_proximity_out").setDropLast(false)
    //Pipeline combine the above
    val pipeline = new Pipeline().setStages(Array(imputer, indexer, encoder))
    val pipelineModel = pipeline.fit(housing_income_cat)
    val housing_temp =  pipelineModel.transform(housing_income_cat).
      withColumn("rooms_per_household", 'total_rooms / 'households).
      withColumn("pop_per_household", 'population / 'households).
      withColumn("bedrooms_per_room", 'total_bedrooms_out / 'total_rooms)

    housing_temp.printSchema()

    housing_temp.filter(isnull('total_bedrooms)).show(5, false)
    //schema so far
    /*
    root
    |-- longitude: float (nullable = true)
    |-- latitude: float (nullable = true)
    |-- housing_median_age: float (nullable = true)
    |-- total_rooms: float (nullable = true)
    |-- total_bedrooms: float (nullable = true)
    |-- population: float (nullable = true)
    |-- households: float (nullable = true)
    |-- median_income: float (nullable = true)
    |-- median_house_value: float (nullable = true)
    |-- ocean_proximity: string (nullable = true)
    |-- income_cat: double (nullable = true)
    |-- total_bedrooms_out: float (nullable = true)
    |-- op_index: double (nullable = true)
    |-- ocean_proximity_out: vector (nullable = true)
    |-- room_per_household: double (nullable = true)
    |-- pop_per_household: double (nullable = true)
    |-- bedrooms_per_room: double (nullable = true
    */

    /*
       Prepare data for StandardScaler that only accept Vector as input
       We use median_house_value as label. Features covers all numeric fields. OP is a vector for categorical data.
       Income_cat is for spliting data in stratified way. Manual manipulation only works for limited features
       'ocean_proximity_out' is linalg.Vector not scala Vector
     */
    val housing_temp2 = housing_temp.map(
      r => (r.getAs[Float]("median_house_value"), Vectors.dense(
        r.getAs[Float]("longitude"), r.getAs[Float]("latitude"), r.getAs[Float]("housing_median_age"), r.getAs[Float]("total_rooms"),
        r.getAs[Float]("total_bedrooms_out"), r.getAs[Float]("population"), r.getAs[Float]("households"), r.getAs[Float]("median_income"),
        r.getAs[Double]("rooms_per_household"), r.getAs[Double]("pop_per_household"), r.getAs[Double]("bedrooms_per_room")
      ),
       r.getAs[Vector]("ocean_proximity_out"), r.getAs[Double]("income_cat"))
    ).toDF("label", "features", "op", "income_cat")

    housing_temp2.printSchema()
    housing_temp2.show(10)

    val scaler = new StandardScaler().setInputCol("features").setOutputCol("scaledFeatures").
      setWithStd(true).setWithMean(true)
    val scalerModel = scaler.fit(housing_temp2)
    val scaledData: Dataset[Row] = scalerModel.transform(housing_temp2)
    scaledData.printSchema()
    scaledData.show(10)


    /*
    Combine numeric vector and categorical vector into one feature vector to prepare for data for modeling
     */

    val combined = scaledData.map(
       r => (r.getAs[Float]("label"),
         new DenseVector(r.getAs[Vector]("scaledFeatures").toArray ++ r.getAs[Vector]("op").toArray),
         r.getAs[Double]("income_cat")
    )).toDF("label", "features", "income_cat")

    val strat_train_temp = combined.stat.sampleBy("income_cat", fractions, 42l)
    val strat_test_temp = combined.except(strat_train_temp)

    val trainDF = strat_train_temp.drop("income_cat")
    val testDF = strat_test_temp.drop("income_cat")

    trainDF.printSchema()
    trainDF.show(10, false)

    /*
      Ready for modeling
     */

    crossValidatorWithLinearRegression(trainDF, testDF)
    crossValidatorWithDecisionTree(trainDF, testDF)

    val cvModel = crossValidatorWithRandomForest(trainDF, testDF)
    val imp_features_value = cvModel.bestModel.asInstanceOf[RandomForestRegressionModel].featureImportances.toArray

    /*
    Array(0.060573039498378244, 0.04765862892710125, 0.038085786043434794, 0.006481938131746838, 0.006393222902016197,
    0.007132552001144698, 0.005379025230251163, 0.3089598383060173, 0.054836496090242694, 0.10789750310754889,
    0.1065564500282175, 0.006052499578144016, 0.23744360585703922, 0.004638894234662272, 0.0019105200640549137, 0.0)
     */

    val fns = housing_temp.schema.fieldNames
    val imp_features = ((fns.take(8) ++ fns.slice(14, 17) ++ op_cat_arr) zip imp_features_value).sortBy(_._2)(Ordering[Double].reverse)

    println(s"RandomForestRegressionModel ranks important features: ${imp_features.mkString(", ")}")


  }

}

