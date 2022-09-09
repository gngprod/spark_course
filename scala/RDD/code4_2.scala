package com.example
package RDD

import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object code4_2 extends App {
  Logger.getLogger("org").setLevel(Level.ERROR)
  val spark = SparkSession.builder()
    .appName("sparkLearn")
    .master("local[*]")
    .getOrCreate()

  import spark.implicits._

  case class Avocado(
                      id: Int,
                      date: String,
                      avgPrice: Double,
                      volume: Double,
                      year: String,
                      region: String)

  val avocadoRDD: RDD[Avocado] = spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv(raw"src\main\resources\avocado.csv")
    .withColumnRenamed("AveragePrice", "avgPrice")
    .withColumnRenamed("TotalVolume", "volume")
    .as[Avocado].rdd
  //  println(" AvocadoRDD:")
  //  avocadoRDD.foreach(println)

  val countRegion = avocadoRDD.map(_.region).distinct().count()
  println(s"\n unique regions = $countRegion")

  println("\n Date > 2018-02-11:")
  avocadoRDD.filter(_.date != null).filter(_.date > "2018-02-11").foreach(println)

  def getMaxCountMonth(avoRDD: RDD[Avocado]): RDD[(String, Int)] = {
    val coun: RDD[(String, Int)] = avoRDD
      .filter(_.date != null)
      .map(ord => (ord.date.split("-")(1), 1))
      .reduceByKey((a, b) => a + b)
    //    coun.foreach(println)
    val maxValue = coun.map(_._2).max()
    coun.filter(_._2 == maxValue)
  }

  val maxCountMonth = getMaxCountMonth(avocadoRDD)
  println(s"\n max count month = $maxCountMonth")

  val maxAvgPrice = avocadoRDD.map(_.avgPrice).max()
  val minAvgPrice = avocadoRDD.map(_.avgPrice).min()
  println(s"\n max and min avg price = $maxAvgPrice and $minAvgPrice")


  val avgVolumeRegion = avocadoRDD
    .map(x => (x.region, x.volume))
    .reduceByKey((a, b) => a + b)
  println("\n avgVolumeRegionRDD:")
  avgVolumeRegion.foreach(println)

  spark.stop()
}
