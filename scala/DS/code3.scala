package DS

import org.apache.spark.sql.{DataFrame, SparkSession}

object code3 extends App {

  //  val spark = SparkSession.builder()
  //    .appName("sparkLearn")
  //    .master("local[*]")
  //    .getOrCreate()
  //
  //  val namesDF: DataFrame = spark.read
  //    .option("header", "true")
  //    .option("inferSchema", "true")
  //    .csv("src/main/resources/names.csv")
  //
  //  namesDF.printSchema()
  //  namesDF.show()
  //
  //  namesDF.filter(col("name") =!= "Bob").show()
  //
  //  namesDF.filter(_ != "Bob").show()
  //
  //  implicit val stringEncoder: Encoder[String] = Encoders.STRING
  //  val namesDS: Dataset[String] = namesDF.as[String]
  //  namesDS.filter(n => n != "Bob")
  //  import spark.implicits._


  val spark = SparkSession.builder()
    .appName("sparkLearn")
    .master("local[*]")
    .getOrCreate()

  case class Customer(
                       name: String,
                       surname: String,
                       age: Int,
                       occupation: String,
                       customer_rating: Double
                     )

  val customersDF: DataFrame = spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv("src/main/resources/customers.csv")

  import spark.implicits._

  val customersDS = customersDF.as[Customer]

  customersDS.printSchema()
  customersDS.show()

  spark.stop()
}
