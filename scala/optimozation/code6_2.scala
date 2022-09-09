package optimozation

import org.apache.spark.sql.SparkSession
import org.apache.log4j.Logger
import org.apache.log4j.Level


object code6_2 extends App {
//  Logger.getLogger("org").setLevel(Level.OFF)
  val spark = SparkSession.builder()
    .appName("sparkLearn")
    .master("local[*]")
    .getOrCreate()

  val DfA = spark.range(1, 100000000).repartition(20)
  val DfB = spark.range(1, 500000000, 5)
  val joinedDF = DfA.join(DfB, "id")
  joinedDF.show()
  System.in.read()
  spark.stop()
}
/*
val DfA = spark.range(1, 10000).repartition(20)
val DfB = spark.range(1, 50000, 5)
val joinedDF = DfA.join(DfB, "id")
joinedDF.take(1)
joinedDF.show()
 */

