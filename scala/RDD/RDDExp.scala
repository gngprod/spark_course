package RDD

import org.apache.spark.sql.SparkSession

import scala.io.Source

object RDDExp extends App {
  val spark = SparkSession.builder()
    .appName("sparkLearn")
    .master("local[*]")
    .getOrCreate()

  case class Avocado(
                      id: Int,
                      Date: String,
                      AveragePrice: Double,
                      TotalVolume: Double,
                      year: String,
                      region: String)

  //11111111111111
  val sc = spark.sparkContext

  def readStores(filename: String): List[Avocado] =
    Source.fromFile(filename).getLines()
      //удаляем первую строчку, тк в ней содержатся названия колонок
      .drop(1)
      // данные в колонках разделяются запятой
      .map(line => line.split(","))
      // построчно считываем данные в case класс
      .map(values => Avocado(
        values(0).toInt,
        values(1),
        values(2).toDouble,
        values(3).toDouble,
        values(12),
        values(13))
      ).toList

  val avocadoRDD1 = sc.parallelize(readStores(raw"src\main\resources\avocado.csv"))
  avocadoRDD1.foreach(println)

  //2222222222
  val avocadoRDD2 = spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv(raw"src\main\resources\avocado.csv")
    .rdd
  avocadoRDD2.foreach(println)

  //3333333333

  import spark.implicits._

  val storesDF = spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv(raw"src\main\resources\avocado.csv")
  storesDF.show()
  val storesDS = storesDF.as[Avocado]
  val avocadoRDD3 = storesDS.rdd
  avocadoRDD3.foreach(println)
}
