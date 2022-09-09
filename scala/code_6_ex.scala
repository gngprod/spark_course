package com.example

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._

object code_6_ex extends App {
  Logger.getLogger("org").setLevel(Level.ERROR)
  val spark = SparkSession.builder()
    .appName("code2_1")
    .master("local[*]")
    .getOrCreate()

  val Schema = StructType(Seq(
    StructField("TRANS_DT", StringType),
    StructField("CARD__NUMBER", StringType),
    StructField("CC_SPEND_CASH", StringType),
    StructField("CC_SPEND_RETAIL", StringType),
    StructField("MCC", StringType)))

  spark.read
    .schema(Schema)
    .option("header", "true")
    .option("sep", ";")
    .csv(raw"C:\ex_6_c.csv")
    .createTempView("T1")

  spark.sql(
    "SELECT * FROM T1"
  ).show()

  spark.sql(
    "SELECT * FROM  (SELECT *, ROW_NUMBER() OVER (PARTITION BY CARD__NUMBER ORDER BY TRANS_DT ASC) as ID FROM T1) AS T2  WHERE ID=1"
  ).show()

  spark.sql(
    "SELECT * FROM  (SELECT *, min(TRANS_DT) OVER (PARTITION BY CARD__NUMBER) as MIN_TRANS_DT  FROM T1) AS T2"
  ).show()
}
