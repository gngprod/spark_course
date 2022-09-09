package com.example

import org.apache.hadoop.io.file.tfile.TFile.Reader.Scanner
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._

object code_3 extends App {
  Logger.getLogger("org").setLevel(Level.ERROR)
  val spark = SparkSession.builder()
    .appName("code2_1")
    .master("local[*]")
    .getOrCreate()

  spark.read
    .option("header", "true")
    .option("sep", ";")
    .csv(raw"C:\ex_3.1_c.csv")
    .createTempView("T1")

  spark.read
    .option("header", "true")
    .option("sep", ";")
    .csv(raw"C:\ex_3.2_c.csv")
    .createTempView("T2")

  spark.sql(
    "SELECT * FROM T1"
  ).show()
  spark.sql(
    "SELECT * FROM T2"
  ).show()

  spark.sql(
    "UPDATE T1 SET T1.Text = T2.text WHERE T1.id = T2.id and T1.Text IS NULL"
  ).show()


}
