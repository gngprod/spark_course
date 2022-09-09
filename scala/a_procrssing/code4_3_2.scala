package a_procrssing

import org.apache.spark.sql.SparkSession
import scala.io.Source

object code4_3_2 extends App {
   val spark = SparkSession.builder()
     .appName("sparkLearn")
     .master("local[*]")
     .getOrCreate()
   val sc = spark.sparkContext

   case class NameColumn(
                          uniq_id: String,
                          product_name: String,
                          manufacturer: String,
                          price: String,
                          number_available: String,
                          number_of_reviews: String)
   def readStores(filename: String): List[NameColumn] =
      Source.fromFile(filename).getLines()
        .drop(1)
        .map(line => line.split(","))
        .map(values => NameColumn(
           values(0),
           values(1),
           values(2),
           values(3),
           values(4),
           values(5))
        ).toList
   val logsDataRDD = sc.parallelize(readStores(raw"src\main\resources\amazon_products.json"))
   logsDataRDD.foreach(println)
}
