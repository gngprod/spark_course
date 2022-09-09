package com.example

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._

object code_5_ex extends App {
  Logger.getLogger("org").setLevel(Level.ERROR)
  val spark = SparkSession.builder()
    .appName("code2_1")
    .master("local[*]")
    .getOrCreate()

  spark.read
    .option("header", "true")
    .option("sep", ";")
    .csv(raw"C:\ex_5_c.csv")
    .createTempView("T1")

  spark.sql(
    "SELECT * FROM T1"
  ).show()

  spark.sql(
    "SELECT name, Day_number, sum(Min) FROM T1 WHERE Diner_flag is NULL GROUP BY name, Day_number"
  ).show()
}
