package com.example

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, count, max}
import org.apache.spark.sql.{Dataset, SaveMode, SparkSession}

object PizzaApp extends App {
  val path1 = args(0)
  val path2 = args(1)
  Logger.getLogger("org").setLevel(Level.ERROR)

  implicit val spark: SparkSession = SparkSession.builder()
    .appName("Pizza App")
    .master("local[*]")
    .getOrCreate()

  val pizzaOrdersDF = spark.read
    .option("inferSchema", "true")
    .option("header", "true")
    .csv(path1)

  val outPizzaOrdersDF = pizzaOrdersDF
    .select("order_type", "address_id")
    .withColumn("orders_total", count("order_type").over(Window.partitionBy("order_type")))
    .withColumn("orders_cnt", count("address_id").over(Window.partitionBy("order_type", "address_id")))
    .withColumn("orders_cnt_max", max("orders_cnt").over(Window.partitionBy("order_type")))
    .orderBy("address_id")

  import spark.implicits._

  case class PizzaOrder(
                         order_type: String,
                         address_id: Int,
                         orders_total: BigInt,
                         orders_cnt: BigInt)

  val outPizzaOrdersDS: Dataset[PizzaOrder] = outPizzaOrdersDF
    .filter(col("orders_cnt") === col("orders_cnt_max"))
    .drop("orders_cnt_max")
    .distinct()
    .as[PizzaOrder]
  outPizzaOrdersDS.show()

  outPizzaOrdersDS.write
    .mode(SaveMode.Overwrite)
    .csv(path2)
}
