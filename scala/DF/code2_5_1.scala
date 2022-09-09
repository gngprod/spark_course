package DF

import org.apache.spark.sql.functions.{avg, col, round, when}
import org.apache.spark.sql.{SaveMode, SparkSession}

object code2_5_1 extends App {
  val spark = SparkSession.builder()
    .appName("sparkLearn")
    .master("local[*]")
    .getOrCreate()

  val mallCustomersDF = spark.read
    .option("inferSchema", "true")
    .option("header", "true")
    .csv(raw"C:\Users\ngzirishvili\IdeaProjects\spark-course\src\main\resources\mall_customers.csv")
  mallCustomersDF.show()

  val newMallCustomersDF = mallCustomersDF.select(
    col("CustomerID")
    , col("Gender")
    , (col("Age") + 2).as("Age")
    , col("Annual Income (k$)")
    , col("Spending Score (1-100)")
  )
  newMallCustomersDF.show()

  val incomeDF = newMallCustomersDF
    .where(col("Age").between(30, 35))
    .groupBy("Gender", "Age")
    .agg(
      round(avg("Annual Income (k$)"), 1)
        .as("Average_Annual_Income_k$")
    )
    .orderBy("Gender", "Age")
  incomeDF.show()

  val newIncomeDF = incomeDF.withColumn(
    "gender_code",
    when(col("Gender") === "Male", 1)
      .when(col("Gender") === "Female", 0)
      .otherwise(null)
  )
  newIncomeDF.show()

  newIncomeDF.write
    .mode(SaveMode.Overwrite)
    .save(raw"C:\Users\ngzirishvili\IdeaProjects\spark-course\src\main\resources\data\customers")
  spark.stop()
}
