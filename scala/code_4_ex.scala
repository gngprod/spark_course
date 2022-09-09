package com.example

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._

object code_4_ex extends App {
  Logger.getLogger("org").setLevel(Level.ERROR)
  val spark = SparkSession.builder()
    .appName("code2_1")
    .master("local[*]")
    .getOrCreate()

  spark.read
    .option("header", "true")
    .option("sep", ";")
    .csv(raw"C:\ex_4_c.csv")
    .createTempView("T1")

  spark.sql(
    "SELECT * FROM T1"
  ).show()

  spark.sql(
    "SELECT * FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY id ORDER BY (SELECT NULL)) as iddup FROM T1) as T2 WHERE iddup > 1"
  ).show()

//    spark.sql(
//      "DELETE T2 FROM (SELECT *, iddup = ROW_NUMBER() OVER (PARTITION BY id ORDER BY (SELECT NULL)) FROM T1) AS T2 WHERE iddup > 1"
//    ).show()

//  spark.sql(
//    "  DELETE T1.* FROM T1, (SELECT *, DupRank = ROW_NUMBER() OVER (PARTITION BY id ORDER BY (SELECT NULL)) FROM T1) AS T2 WHERE DupRank > 1"
//  ).show()

  spark.sql(
    "WITH C AS ( SELECT *, ROW_NUMBER() OVER(PARTITION BY id ORDER BY (SELECT NULL)) AS n FROM T1) DELETE FROM C WHERE n > 1"
  ).show()

}
