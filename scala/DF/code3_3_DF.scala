package DF

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.{DataFrame, SparkSession}

object code3_3_DF extends App {
  val spark = SparkSession.builder()
    .appName("sparkLearn")
    .master("local[*]")
    .getOrCreate()

  val AiJobsIndustryDF: DataFrame = spark.read
    .option("multiLine", "true")
    .option("header", "true")
    .option("inferSchema", "true")
    .csv(raw"src\main\resources\AiJobsIndustry.csv")
    .na.drop()
    .drop("Link")
    .withColumn("Company", split(col("Company"), raw"\n")(1))

  def extractIntegerValue(df: DataFrame): DataFrame = {
    df.withColumn("CompanyReviews",
      regexp_replace(
        e = regexp_extract(
          e = col("CompanyReviews"),
          exp = "([\\d\\.\\,\\s\\xA0]+)\\s?.*",
          groupIdx = 1),
        pattern = "\\D",
        replacement = ""
      ).cast(IntegerType)
    )
  }

  def createDFOut(df: DataFrame): DataFrame = {
    def createRaw(df: DataFrame, stType: String, ctType: String): DataFrame = {
      val agr = if (ctType == "max") {
        val CompanySumDF = df.groupBy(stType)
          .agg {
            sum("CompanyReviews").as("sumCompanyReviews")
          }
        CompanySumDF
          .where(col("sumCompanyReviews") === CompanySumDF.agg(max("sumCompanyReviews")).collect()(0)(0))
      } else {
        val CompanySumDF = df.groupBy(stType)
          .agg {
            sum("CompanyReviews").as("sumCompanyReviews")
          }
        CompanySumDF
          .where(col("sumCompanyReviews") === CompanySumDF.agg(min("sumCompanyReviews")).collect()(0)(0))
      }
      agr
        .withColumnRenamed(stType, "name")
        .withColumn("status_type", lit(stType))
        .withColumnRenamed("sumCompanyReviews", "count")
        .withColumn("count_type", lit(ctType))
        .join(df, agr.col(stType) === df.col(stType))
        .groupBy("name", "status_type", "count", "count_type")
        .agg(concat_ws(", ", collect_list("Location")) as "location")
        .limit(1)
    }

    val companyMax = createRaw(df, "Company", "max")
    val companyMin = createRaw(df, "Company", "min")
    val jtMax = createRaw(df, "JobTitle", "max")
    val jtMin = createRaw(df, "JobTitle", "min")
    companyMax.union(companyMin).union(jtMax).union(jtMin)
      .select("name", "status_type", "location", "count", "count_type")
  }

  val AiJobsIndustryDFOut: DataFrame = AiJobsIndustryDF
    .transform(extractIntegerValue)
    .transform(createDFOut)
  AiJobsIndustryDFOut.show()

  spark.stop()
}

/*
|Amazon.com    |Company    |Location1  |200000  |max|
|Amazon.com    |Company    |Location2  |1       |min|
|DataScientist |JobTitle   |Location3  |500000  |max|
|DataScientist |JobTitle   |Location4  |2       |min|
 */
