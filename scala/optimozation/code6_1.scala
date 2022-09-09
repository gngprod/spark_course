package optimozation

import org.apache.spark.sql.{DataFrame, SparkSession}

object code6_1 extends App {
  val spark = SparkSession.builder()
    .appName("sparkLearn")
    .master("local[*]")
    .getOrCreate()

  val employeeDF: DataFrame =
    spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("src/main/resources/employee2.csv")

  val outEmployeeDF = employeeDF.groupBy("department").avg("salary")
  outEmployeeDF.explain()
  System.in.read()
  spark.stop()
}
/*
id,salary,department
0,246,Agriculture
1,764,Commerce
2,25,Defense
3,478,Education
4,230,Energy
5,200,Commerce
6,67,Defense
7,693,Defense
8,110,Education
9,684,Energy
10,263,Commerce
11,556,Agriculture
 */
