package RDD

import org.apache.spark.sql.SparkSession

object code5_3_2 extends App {
  val spark = SparkSession.builder()
    .appName("sparkLearn")
    .master("local[*]")
    .getOrCreate()

  import org.apache.spark.sql.functions._

  val employeesDF = spark.read.option("inferSchema", "true").option("header", "true").csv(raw"/opt/spark-data/employee.csv")
  val outDF = employeesDF.select(col("name"), length(col("name")).as("length"))
  outDF.show()
}
