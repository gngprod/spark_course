package DF

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, when}

object code2_3_2 extends App {
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
    .withColumn(
      "is_workday",
      when(col("HOLIDAY") === "Holiday"
        and col("FUNCTIONING_DAY") === "No", 0)
        .otherwise(1)
    )
    .select(
      "HOLIDAY"
      , "FUNCTIONING_DAY"
      , "is_workday"
    )
    .distinct()
    .show()

  spark.stop()
}
