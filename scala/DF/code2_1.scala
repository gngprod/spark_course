package DF

/**
 * 1 задание
 */
//object code2_1_1 extends App {
//
//  import org.apache.spark.sql.SparkSession
//  import spark.implicits._
//
//  val spark = SparkSession.builder()
//    .appName("code2_1")
//    .master("local[*]")
//    .getOrCreate()
//  val df = Seq(
//    ("s9FH4rDMvds", "2020-08-11T22:21:49Z", "UCGfBwrCoi9ZJjKiUK8MmJNw", "2020-08-12T00:00:00Z"),
//    ("kZxn-0uoqV8", "2020-08-11T14:00:21Z", "UCGFNp4Pialo9wjT9Bo8wECA", "2020-08-12T00:00:00Z"),
//    ("QHpU9xLX3nU", "2020-08-10T16:32:12Z", "UCAuvouPCYSOufWtv8qbe6wA", "2020-08-12T00:00:00Z")
//  )
//  val coursesDF = df.toDF("videoId", "publishedAt", "channelId", "trendingDate")
//  coursesDF.show()
//  spark.stop()
//}

/**
 * 2 задание
 */
object code2_1_2 extends App {

  import org.apache.spark.sql.{Row, SparkSession}

  val spark = SparkSession.builder()
    .appName("code2_1")
    .master("local[*]")
    .getOrCreate()
  val restaurantDF = spark.read
    .format("json") // проинструктировали spark reader прочитать файлв формате json
    .option("inferSchema", "true") // предоставляем спарку самому составить схему данных
    .load("C:\\Users\\ngzirishvili\\IdeaProjects\\spark-course\\src\\main\\resources\\iris.json") // указываем путь к файлу
  restaurantDF.show(2)
  restaurantDF.printSchema()
  val restaurantArray: Array[Row] = restaurantDF.take(1)
  restaurantArray.foreach(println)
  spark.stop()
}

/**
 * 3 задание
 */
object code2_1_3 extends App {

  import org.apache.spark.sql.SparkSession
  import org.apache.spark.sql.types._

  val spark = SparkSession.builder()
    .appName("code2_1")
    .master("local[*]")
    .getOrCreate()

  val restaurant_exSchema = StructType(Seq(
    StructField("average_cost_for_two", LongType),
    StructField("cuisines", StringType),
    StructField("deeplink", StringType),
    StructField("has_online_delivery", IntegerType),
    StructField("is_delivering_now", IntegerType),
    StructField("menu_url", StringType),
    StructField("name", StringType),
    StructField("opened", StringType),
    StructField("photos_url", StringType),
    StructField("url", StringType),
    StructField("user_rating",
      StructType(Seq(
        StructField("aggregate_rating", StringType),
        StructField("rating_color", StringType),
        StructField("rating_text", StringType),
        StructField("votes", StringType)
      )))
  ))

  val restaurant_exDF = spark.read
    .schema(restaurant_exSchema)
    .json("C:\\Users\\ngzirishvili\\IdeaProjects\\spark-course\\src\\main\\resources\\restaurant_ex.json")
  restaurant_exDF.show(2)
  restaurant_exDF.printSchema()
}

