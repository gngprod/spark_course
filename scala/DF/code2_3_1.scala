package DF

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col

object code2_3_1 extends App {
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

  bikeSharingDF.select(
    col("Hour")
    , col("TEMPERATURE")
    , col("HUMIDITY")
    , col("WIND_SPEED")
  )
    .show(3)

  //  val count_day = bikeSharingDF
  //    .where(col("RENTED_BIKE_COUNT") === 254)
  //    .where(col("TEMPERATURE") > 0)
  //    .count()
  //
  //  println(count_day)

  spark.stop()
}
