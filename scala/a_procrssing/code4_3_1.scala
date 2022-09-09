package com.example
package a_procrssing

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{Column, SparkSession}
import org.apache.spark.sql.functions._

import scala.io.Source

object code4_3_1 extends App {
  Logger.getLogger("org").setLevel(Level.ERROR)
  val spark = SparkSession.builder()
    .appName("sparkLearn")
    .master("local[*]")
    .getOrCreate()
  val sc = spark.sparkContext

  case class NameColumn(
                       id: Int,
                       host: String,
                       time: String,
                       method: String,
                       url: String,
                       response: String,
                       bytes: Int)
  def readStores(filename: String): List[NameColumn] =
    Source.fromFile(filename).getLines()
      .drop(1)
      .map(line => line.split(","))
      .map(values => NameColumn(
        values(0).toInt,
        values(1),
        values(2),
        values(3),
        values(4),
        values(5),
        values(6).toInt)
      ).toList
  val logsDataRDD = sc.parallelize(readStores(raw"src\main\resources\logs_data.csv"))


//  //  1
//  logsDataRDD.take(20).foreach(println)
//  println(logsDataRDD.map(_.id).count())
//  println(logsDataRDD.map(_.host).count())
//  println(logsDataRDD.map(_.time).count())
//  println(logsDataRDD.map(_.method).count())
//  println(logsDataRDD.map(_.url).count())
//  println(logsDataRDD.map(_.response).count())
//  println(logsDataRDD.map(_.bytes).count())

//  //  2
//  val countResponse = logsDataRDD
//    .filter(_.response.replaceAll("[0-9]", "").isEmpty)
//    .map(ord => (ord.response, 1))
//    .reduceByKey((a, b) => a + b)
//  countResponse.foreach(println)

//  //  3
//  val sumBytes = logsDataRDD.map(_.bytes).sum()
//  val maxBytes = logsDataRDD.map(_.bytes).max()
//  val minBytes = logsDataRDD.map(_.bytes).min()
//  val countBytes = logsDataRDD.map(_.bytes).count()
//  val avgBytes = sumBytes/countBytes
//  println(s"sum bytes: $sumBytes")
//  println(s"avg bytes: $avgBytes")
//  println(s"max bytes: $maxBytes")
//  println(s"min bytes: $minBytes")

  //  4
//  val trendHost = logsDataRDD
//      .map(ord => (ord.host, 1))
//      .reduceByKey((a, b) => a + b)
//  val maxValue = trendHost.map(_._2).max()
//  trendHost.filter(_._2 == maxValue).foreach(println)
//  trendHost.take(20).foreach(println)

//  // 5
//  val top3Host = trendHost.sortBy(_._2, ascending = false).take(3)
//  top3Host.foreach(println)

  //  6
//  val format = new java.text.SimpleDateFormat("yyyy-MM-dd")
//  logsDataRDD.map(_.time).get

//  val data = logsDataRDD.map(x => x.time.SimpleDateFormat("%Y-%m-%d %H:%M:%S"))

//
//  def transformDate(date: Column): Column = {
//    datediff(second(date), date_format(lit("1900-01-01 00:00:00"), "%Y-%m-%d %H:%M:%S"))
//  }
//  transformDate(lit("805465029")).foreach(println)


//  val format = new java.text.SimpleDateFormat("%Y-%m-%d %H:%M:%S")
//  val dataRDD = logsDataRDD.map( x => x.time +
//  dataRDD.take(20).foreach(println)


//  val dateStr = "2021-06-13"
//  val formatter = DateTimeFormat.forPattern("yyyy-MM-dd")
//  val dateTime:DateTime = formatter.parseDateTime(dateStr)


//  val data2 = logsDataRDD.map(x => format(x.time))  //,date_format(x.time.toInt, "%Y-%m-%d %H:%M:%S")) //datetime.datetime.fromtimestamp(x.time.toInt).strftime("%Y-%m-%d %H:%M:%S"))
//  data2.take(20).foreach(println)
}
