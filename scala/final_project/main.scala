package com.example
package final_project

object main extends App {
  Logger.getLogger("org").setLevel(Level.ERROR)
  val spark = SparkSession.builder()
    .appName("sparkLearn")
    .master("local[*]")
    .getOrCreate()


}
