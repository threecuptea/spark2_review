package org.freemind.spark.sql

import org.apache.spark.sql.{Column, Row, SparkSession}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{LongType, StructField, StructType}

/**
  *
  * References
  *
  * https://databricks.com/blog/2015/07/15/introducing-window-functions-in-spark-sql.html
  *
  * https://alvinhenrick.com/2017/05/16/apache-spark-analytical-window-functions/
  *
  * * Try 2.12.8 failed.  got Exception in thread "main" java.lang.BootstrapMethodError: java.lang.NoClassDefFoundError: scala/runtime/SymbolLiteral
  * I have to revert to 2.11
  *
  * $SPARK_HOME/bin/spark-submit --master local[4] --class org.freemind.spark.sql.WindowFunctions target/scala-2.11/spark2_review_2.11-0.1.jar
  *
  * @author sling/ threecuptea, 2018/10/27, 2018/12/30 (re-practice)
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

    val wsByCategoryOrderByRevenue = Window.partitionBy('category).orderBy('revenue.desc)

    //the best selling 2 product in a category,
    //Rank is our regular way.  Dense_rank leaves no gaps in ranking sequence when there are ties, 1,2,2,2, 3 insteadof 1,2,2,2,5
    println("Display the best 2 products in each category")
    productDF.select('*, dense_rank().over(wsByCategoryOrderByRevenue).as("rank")).filter('rank <= 2).show(false)
    //What is the difference between the revenue of each product and the revenue of the best-selling product in the same category of that product
    val revenueDiff: Column = first('revenue).over(wsByCategoryOrderByRevenue) - 'revenue
    println("Display the difference between the revenue of each product and the revenue of the best-selling product in the same category as that product")
    productDF.select('*, revenueDiff.as("revenue_diff")).show(false)

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

    val wsByDeptOrderBySal = Window.partitionBy('deptno).orderBy('sal.desc)

    println("Display id with employees repartition(2) first before using monotonically_increasing_id")
    val repartRdd = empDF.rdd.repartition(2)
    spark.createDataFrame(repartRdd, empDF.schema).select((monotonically_increasing_id() + 1).as("id"), '*).show(false)
    //The result is (Row, Long)
    val rddWithId = repartRdd.zipWithUniqueId().map {
      //+: prepend and :+ append.  An element will be added to the + side is located
      case (r: Row, id: Long) => Row.fromSeq((id+1) +: r.toSeq)
    }
    println("Display id with employees repartition(2) first before using rdd.zipWithUniqueId")
    spark.createDataFrame(rddWithId, StructType(StructField("id", LongType, false) +: empDF.schema.fields)).show(false)
    //The result is (Row, Long)
    val rddWithIdx = repartRdd.zipWithIndex().map {
      case (r: Row, idx: Long) => Row.fromSeq((idx+1) +: r.toSeq)
    }
    println("Display id with employees repartition(2) first before using rdd.zipWithIndex")
    spark.createDataFrame(rddWithIdx, StructType(StructField("id", LongType, false) +: empDF.schema.fields)).show(false)

    //rank
    println("Display employees with rank by Deptno and order by salary")
    empDF.select('*, rank().over(wsByDeptOrderBySal).as("rank")).show(false)
    //dense_rank
    println("Display employees with dense_rank by Deptno and order by salary")
    empDF.select('*, dense_rank().over(wsByDeptOrderBySal).as("dense_rank")).show(false)
    //row_number
    println("Display employees with row_numer by Deptno and order by salary")
    empDF.select('*, row_number().over(wsByDeptOrderBySal).as("row_number")).show(false)
    //running_total, the same value in range will be included???
    println("Display employees with running_total (salary)  by Deptno and order by salary. Surprise!!!")
    empDF.select('*, sum('sal).over(wsByDeptOrderBySal).as("runnning_total")).show(false)
    //lead, the following row, returns the value that is offset rows after the current row
    println("Display employees with lead (next_row_value)  by Deptno and order by salary")
    empDF.select('*, lead('sal, 1, null).over(wsByDeptOrderBySal).as("next_row_value")).show(false)
    //lag, the preceding row, returns the value that is offset rows before the current row
    println("Display employees with lag (prev_row_value)  by Deptno and order by salary")
    empDF.select('*, lag('sal, 1, null).over(wsByDeptOrderBySal).as("prev_row_value")).show(false)
    //first
    println("Display employees with first in partition by Deptno and order by salary")
    empDF.select('*, first('sal).over(wsByDeptOrderBySal).as("first_value")).show(false)
    //last
    println("Display employees with last in partition by Deptno and order by salary w/o unbounded following. (The default window frame is range between unbounded preceding and current row)")
    empDF.select('*, last('sal).over(wsByDeptOrderBySal).as("last_value")).show(false)
    //We get unexpected result last := sal
    //This happens because the default window frame is range between unbounded preceding and current row,
    //It does not include any row beyond the current row. I tried unbounded on both ends and does not work
    val wsByDeptOrderBySalUnbounded = wsByDeptOrderBySal.rowsBetween(Window.currentRow, Window.unboundedFollowing)
    println("Display employees with last in partition by Deptno and order by salary with unbounded following")
    empDF.select('*, last('sal).over(wsByDeptOrderBySalUnbounded).as("last_value")).show(false)

    spark.stop()





  }

}
