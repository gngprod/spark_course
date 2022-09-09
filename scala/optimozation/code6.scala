package optimozation

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

object code6 extends App {
  val spark = SparkSession.builder()
    .appName("sparkLearn")
    .master("local[*]")
    .getOrCreate()

  val numbersDF: DataFrame =
    spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("src/main/resources/numbers.csv")


  def getSquare: String => Int = (num: String) =>
    num.split("_")(1).toInt


  val getSquareUDF = udf(getSquare)

  val squaresDF = numbersDF.select(
    col("id"),
    getSquareUDF(col("number")).as("number"))

  squaresDF.show(5)
  squaresDF.explain()

  val squaresDF2 = numbersDF
    .select(getSquareUDF(col("number")).as("number"))

  squaresDF2.explain
}
