package DF

import org.apache.spark.sql.types._
import org.apache.spark.sql.{SaveMode, SparkSession}

object code2_2 extends App {

  val spark = SparkSession.builder()
    .appName("code2_1")
    .master("local[*]")
    .getOrCreate()

  val moviesSchema = StructType(Seq(
    StructField("n/a", IntegerType),
    StructField("show_id", StringType),
    StructField("type", StringType),
    StructField("title", StringType),
    StructField("director", StringType),
    StructField("cast", StringType),
    StructField("country", StringType),
    StructField("date_added", DateType),
    StructField("release_year", IntegerType),
    StructField("rating", StringType),
    StructField("duration", IntegerType),
    StructField("listed_in", StringType),
    StructField("description", StringType),
    StructField("year_added", IntegerType),
    StructField("month_added", DoubleType),
    StructField("season_count", IntegerType)
  ))

  val movies_on_netflixDF = spark.read
    .schema(moviesSchema)
    .option("header", "true")
    .option("sep", ",")
    .option("nullValue", "n/a")
    .csv(raw"C:\Users\ngzirishvili\IdeaProjects\spark-course\src\main\resources\movies_on_netflix.csv")
  movies_on_netflixDF.printSchema()
  movies_on_netflixDF.show()

  movies_on_netflixDF.write
    .mode(SaveMode.Overwrite)
    .save(raw"C:\Users\ngzirishvili\IdeaProjects\spark-course\src\main\resources\data")
}
