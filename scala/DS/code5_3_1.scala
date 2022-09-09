package DS

import org.apache.spark.sql.SparkSession

object code5_3_1 extends App {
  val spark = SparkSession.builder()
    .appName("sparkLearn")
    .master("local[*]")
    .getOrCreate()

  val sc = spark.sparkContext
  val rangeRDD = sc.range(1, 100001)
  val numPartitions = rangeRDD.partitions.length
  val sumRDD = rangeRDD.sum()

  spark.stop()
}
