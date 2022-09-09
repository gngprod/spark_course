package com.example

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._

object code_1_2 extends App {
  Logger.getLogger("org").setLevel(Level.ERROR)
  val spark = SparkSession.builder()
    .appName("code2_1")
    .master("local[*]")
    .getOrCreate()

  spark.read
    .option("header", "true")
    .option("sep", ";")
    .csv(raw"C:\ex_1.1_c.csv")
    .createTempView("T1")

  spark.read
    .option("header", "true")
    .option("sep", ";")
    .csv(raw"C:\ex_1.2_c.csv")
    .createTempView("T2")

  spark.sql(
    "SELECT * FROM T1"
  ).show()

  spark.sql(
    "SELECT * FROM T2"
  ).show()

  // 1
  spark.sql(
    "Select name, m_name from T1 join T2 on T1.id=T2.id"
  ).show()

  // 2
  spark.sql(
    "Insert into table T1    select * from T2    where id not in (select id from T1)"
  ).show()


}
