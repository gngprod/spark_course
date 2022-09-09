package DF

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, max, min}

object code2_3_3 extends App {
  val spark = SparkSession.builder()
    .appName("code2_1")
    .master("local[*]")
    .getOrCreate()

  val bikeSharingDF = spark.read
    .option("inferSchema", "true")
    .option("header", "true")
    .option("sep", ",")
    .option("nullValue", "n/a")
    .csv(raw"C:\Users\ngzirishvili\IdeaProjects\spark-course\src\main\resources\bike_sharing.csv")

  bikeSharingDF
    .groupBy("Date")
    .agg(
      min(col("TEMPERATURE")).as("min_temp")
      , max(col("TEMPERATURE")).as("max_temp")
    )
    .show(3)

  spark.stop()
}
