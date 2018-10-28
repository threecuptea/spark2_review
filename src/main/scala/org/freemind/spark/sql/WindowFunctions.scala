package org.freemind.spark.sql

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

/**
  *
  * References
  *
  * https://databricks.com/blog/2015/07/15/introducing-window-functions-in-spark-sql.html
  *
  * https://alvinhenrick.com/2017/05/16/apache-spark-analytical-window-functions/
  *
  * @author sling/ threecuptea, 2018/10/27
  */
object WindowFunctions {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName("WindowsFunction").config("spark.sql.shuffle.partitions", 1).getOrCreate()
    import spark.implicits._

    val productDF = spark.createDataFrame(Seq(
      ("Thin", "Cell phone", 6000),
      ("Normal", "Tablet", 1500),
      ("Mini", "Tablet", 5500),
      ("Ultra Thin", "Cell phone", 5000),
      ("Very Thin", "Cell phone", 6000),
      ("Big", "Tablet", 2500),
      ("Bendable", "Cell phone", 3000),
      ("Foldable", "Cell phone", 3000),
      ("Pro", "Tablet", 4500),
      ("Pro2", "Tablet", 6500)
    )).toDF("product", "category", "revenue")

    val windowSpec1 = Window.partitionBy("category").orderBy(desc("revenue"))

    //the best selling 2 product in a category
    productDF.select('product, 'category, 'revenue, dense_rank.over(windowSpec1).as("rank")).filter('rank <= 2).show(false)
    //What is the difference between the revenue of each product and the revenue of the best-selling product in the same category of that product
    val revenue_diff = first('revenue).over(windowSpec1) - 'revenue

    productDF.select('product, 'category, 'revenue, revenue_diff.as("revenue_diff")).show(false)


    val empDF = spark.createDataFrame(Seq(
      (7369, "SMITH", "CLERK", 7902, "17-Dec-80", 800, 20, 10),
      (7499, "ALLEN", "SALESMAN", 7698, "20-Feb-81", 1600, 300, 30),
      (7521, "WARD", "SALESMAN", 7698, "22-Feb-81", 1250, 500, 30),
      (7566, "JONES", "MANAGER", 7839, "2-Apr-81", 2975, 0, 20),
      (7654, "MARTIN", "SALESMAN", 7698, "28-Sep-81", 1250, 1400, 30),
      (7698, "BLAKE", "MANAGER", 7839, "1-May-81", 2850, 0, 30),
      (7782, "CLARK", "MANAGER", 7839, "9-Jun-81", 2450, 0, 10),
      (7788, "SCOTT", "ANALYST", 7566, "19-Apr-87", 3000, 0, 20),
      (7839, "KING", "PRESIDENT", 0, "17-Nov-81", 5000, 0, 10),
      (7844, "TURNER", "SALESMAN", 7698, "8-Sep-81", 1500, 0, 30),
      (7876, "ADAMS", "CLERK", 7788, "23-May-87", 1100, 0, 20)
    )).toDF("empno", "ename", "job", "mgr", "hiredate", "sal", "comm", "deptno")

    //I cannot change frame here, otherwise error occured in rank
    val partitionWindow = Window.partitionBy('deptno).orderBy('sal.desc)

    //rank
    empDF.select('*, rank.over(partitionWindow).as("rank")).show(false)
    //dense_rank
    empDF.select('*, dense_rank.over(partitionWindow).as("dense_rank")).show(false)
    //dense_rank
    empDF.select('*, row_number.over(partitionWindow).as("row_number")).show(false)
    //lead, display the next in the sequence
    empDF.select('*, lead('sal, 1, null).over(partitionWindow).as("lead")).show(false)
    //lag
    empDF.select('*, lag('sal, 1, null).over(partitionWindow).as("lag")).show(false)
    //first
    empDF.select('*, first('sal).over(partitionWindow).as("first_val")).show(false)

    //This happens because default window frame is range between unbounded preceding and current row,
    //so the last_value() never looks beyond current row unless I change the frame.
    //If I don't change row frame, last_val will display itself
    val partitionWindowWithUnbound = Window.partitionBy('deptno).orderBy('sal.desc).rowsBetween(Window.currentRow, Window.unboundedFollowing)
    //last
    empDF.select('*, last('sal).over(partitionWindowWithUnbound).as("last_val")).show(false)





  }

}
