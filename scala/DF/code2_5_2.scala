package DF

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

object code2_5_2 extends App {
  val spark = SparkSession.builder()
    .appName("sparkLearn")
    .master("local[*]")
    .getOrCreate()

  val subtitlesSchema = StructType(Seq(StructField("data_main", StringType)))

  val subtitlesS1DF = spark.read
    .schema(subtitlesSchema)
    .option("inferSchema", "true")
    .option("header", "true")
    .csv(raw"C:\Users\ngzirishvili\IdeaProjects\spark-course\src\main\resources\subtitles_s1.json")

  val subtitlesS2DF = spark.read
    .schema(subtitlesSchema)
    .option("inferSchema", "true")
    .option("header", "true")
    .csv(raw"C:\Users\ngzirishvili\IdeaProjects\spark-course\src\main\resources\subtitles_s2.json")

  def transformCode(df: DataFrame): DataFrame = {
    val split_col = split(df("data_main"), ":")
    val df2 = df
      .withColumn("series", split_col(0))
      .withColumn("word", split_col(1))
      .drop("data_main")
    val df3 = df2
      .select(
        col("series"),
        explode(split(lower(col("word")), "\\W+")).alias("word")
      )
      .where(col("word") =!= "")
    val df4 = df3.groupBy("word").count().orderBy(col("count").desc)
    val windowSpec = Window.orderBy(col("count").desc)
    df4.withColumn("id", row_number.over(windowSpec))
  }

  val outSubtitlesS1DF = subtitlesS1DF
    .transform(transformCode)
    .withColumnRenamed("word", "w_s1")
    .withColumnRenamed("count", "cnt_s1")
    .withColumn("id", col("id") - 1)
    .limit(20)

  val outSubtitlesS2DF = subtitlesS2DF.transform(transformCode)
    .withColumnRenamed("word", "w_s2")
    .withColumnRenamed("count", "cnt_s2")
    .withColumn("id", col("id") - 1)
    .limit(20)

  val on = outSubtitlesS1DF("id") === outSubtitlesS2DF("id")
  val finalOut = outSubtitlesS1DF.join(outSubtitlesS2DF, on)
    .drop(outSubtitlesS2DF("id"))
  finalOut.show()

  finalOut.write
    .mode(SaveMode.Overwrite)
    .json(raw"C:\Users\ngzirishvili\IdeaProjects\spark-course\src\main\resources\data\wordcount")

  spark.stop()
}
