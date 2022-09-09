package RDD

import org.apache.spark.sql.SparkSession

object code5 {
  def main(args: Array[String]): Unit = {

    if (args.length != 1) {
      println("Specify the path to the File")
      System.exit(1)
    }

    val spark = SparkSession.builder()
      .appName("Employee App")
      .getOrCreate()

    val employeesDF = spark.read
      .option("inferSchema", "true")
      .option("header", "true")
      .csv(args(0))


    employeesDF.printSchema()

    employeesDF.show()
  }
}

//implicit val logger: Logger = Logger.getLogger(getClass)
//  spark.sparkContext.setLogLevel("ERROR")