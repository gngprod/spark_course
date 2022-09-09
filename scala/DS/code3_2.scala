package DS

import org.apache.spark.sql.functions.{coalesce, col}
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

object code3_2 extends App {
  val spark = SparkSession.builder()
    .appName("sparkLearn")
    .master("local[*]")
    .getOrCreate()

  case class Shoes(
                    item_category: String,
                    item_name: String,
                    item_after_discount: String,
                    item_price: String,
                    percentage_solds: Int,
                    item_rating: Int,
                    item_shipping: String,
                    buyer_gender: String
                  )

  val athleticShoesDF: DataFrame = spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv("src/main/resources/athletic_shoes.csv")

  import spark.implicits._

  val athleticShoesDS: Dataset[Shoes] = athleticShoesDF.as[Shoes]

  val newAthleticShoesDS = athleticShoesDS
    .na.drop(List("item_name", "item_category"))
    .na.fill(Map(
    "item_price" -> "n/a",
    "percentage_solds" -> 0,
    "item_rating" -> 0,
    "item_shipping" -> "n/a",
    "buyer_gender" -> "unknown"
  ))
    .select(
      col("item_category"),
      col("item_name"),
      coalesce(col("item_after_discount"), col("item_price"))
        .as("item_after_discount"),
      col("item_price"),
      col("percentage_solds"),
      col("item_rating"),
      col("item_shipping"),
      col("buyer_gender")
    )

  newAthleticShoesDS.show()

  spark.stop()
}
