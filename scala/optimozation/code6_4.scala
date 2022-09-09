package optimozation

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{abs, avg, row_number}
import org.apache.spark.sql.{DataFrame, SparkSession}

object code6_4 extends App {

  val spark = SparkSession.builder()
    .appName("sparkLearn")
    .master("local[*]")
    .getOrCreate()
  spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)

  val freelancersDF: DataFrame =
    spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("src/main/resources/freelancers.csv")

  val offersDF: DataFrame =
    spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("src/main/resources/offers.csv")

  val unluckyOut = freelancersDF.join(offersDF, Seq("category", "city"))
    .filter(abs(freelancersDF.col("experienceLevel") - offersDF.col("experienceLevel")) <= 1)
    .groupBy("id")
    .agg(avg("price").as("avgPrice"))
  unluckyOut.show()
  unluckyOut.explain()

  val windowFunc = Window.partitionBy("category", "city").orderBy("category", "city")
  val freelancersDF2 = freelancersDF.withColumn("sortnum", row_number.over(windowFunc))
  val offersDF2 = offersDF.withColumn("sortnum", row_number.over(windowFunc))
  val luckyOut = freelancersDF2.join(offersDF2, "sortnum")
    .filter(abs(freelancersDF2.col("experienceLevel") - offersDF2.col("experienceLevel")) <= 1)
    .groupBy("id")
    .agg(avg("price").as("avgPrice"))
  luckyOut.show()
  luckyOut.explain()

  System.in.read()
  spark.stop()
}
