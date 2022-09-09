package DS

import org.apache.spark.sql.{Dataset, SparkSession}

object code3_4_1 extends App {
  val spark = SparkSession.builder()
    .appName("sparkLearn")
    .master("local[*]")
    .getOrCreate()

  import spark.implicits._

  case class Cars(
                   id: Int,
                   price: Int,
                   brand: String,
                   type_cars: String,
                   mileage: Option[Double],
                   color: String,
                   date_of_purchase: String
                 )

  val carsDS: Dataset[Cars] = spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv(raw"src\main\resources\cars.csv")
    .withColumnRenamed("type", "type_cars")
    .as[Cars]

  //  def avgMileageCol(ds: Dataset[Cars]): Dataset[CarsMilOut] = {
  //
  //    val milDS: Dataset[CarsMileage] = ds
  //      .map(ord => Cars(
  //        ord.id, ord.price, ord.brand, ord.type_cars,
  //        ord.mileage.orElse(Option(0.0)),
  //        ord.color, ord.date_of_purchase))
  //      .withColumn("avg_mileage", avg(col("mileage")).over()).as[CarsMileage]
  //      .map(ord => CarsMileage(
  //        ord.id, ord.avg_mileage))
  //
  //    ds.joinWith(milDS, ds.col("id") === milDS.col("id"))
  //      .map(ord => CarsMilOut(
  //        ord._1.id, ord._1.price, ord._1.brand, ord._1.type_cars,
  //        ord._1.mileage, ord._1.color, ord._1.date_of_purchase, ord._2.avg_mileage))
  //  }
  //
  //  def yearsCol( ds:Dataset[Cars]): Dataset[CarsYearsOut] = {
  //    val formatDateCol = coalesce(
  //      to_date(col("date_of_purchase"), "yyyy-MM-dd"),
  //      to_date(col("date_of_purchase"), "yyyy MM dd"),
  //      to_date(col("date_of_purchase"), "yyyy MMM dd"),
  //      lit("Unknown Format"))
  //
  //    val carsYears:Dataset[CarsYears] = ds
  //    .withColumn("actual_date", formatDateCol)
  //    .withColumn("years_since_purchase", round(datediff(current_date(), col("actual_date"))/365)).as[CarsYears]
  //    .map(ord => CarsYears(ord.id, ord.years_since_purchase))
  //
  //    ds.joinWith(carsYears, ds.col("id") === carsYears.col("id"))
  //        .map( ord =>
  //          CarsYearsOut(
  //            ord._1.id, ord._1.price, ord._1.brand, ord._1.type_cars,
  //            ord._1.mileage, ord._1.color, ord._1.date_of_purchase, ord._2.years_since_purchase))
  //  }
  //  case class CarsMileage(
  //                        id: Int,
  //                        avg_mileage: Double
  //                      )
  //  case class CarsMilOut(
  //                      id: Int,
  //                      price: Int,
  //                      brand: String,
  //                      type_cars: String,
  //                      mileage: Option[Double],
  //                      color: String,
  //                      date_of_purchase: String,
  //                      avg_mileage: Double)
  //  val carsDSMileage: Dataset[CarsMilOut] = carsDS
  //    .transform(avgMileageCol)
  //
  //  case class CarsYears(
  //                        id: Int,
  //                        years_since_purchase: Double)
  //  case class CarsYearsOut(
  //                         id: Int,
  //                         price: Int,
  //                         brand: String,
  //                         type_cars: String,
  //                         mileage: Option[Double],
  //                         color: String,
  //                         date_of_purchase: String,
  //                         years_since_purchase: Double)
  //  val carsDSYears: Dataset[CarsYearsOut] = carsDS
  //    .transform(yearsCol)
  //
  //  case class CarsOut(
  //                           id: Int,
  //                           price: Int,
  //                           brand: String,
  //                           type_cars: String,
  //                           mileage: Option[Double],
  //                           color: String,
  //                           date_of_purchase: String,
  //                           avg_mileage: Double,
  //                           years_since_purchase: Double)
  //  val outCarsDS: Dataset[CarsOut] =
  //    carsDSMileage.joinWith(carsDSYears, carsDSMileage.col("id") === carsDSYears.col("id"))
  //    .map( ord =>
  //      CarsOut(
  //        ord._1.id, ord._1.price, ord._1.brand, ord._1.type_cars,
  //        ord._1.mileage, ord._1.color, ord._1.date_of_purchase, ord._1.avg_mileage, ord._2.years_since_purchase))
  //  outCarsDS.show()


  case class CarsOuts(
                       id: Int,
                       price: Int,
                       brand: String,
                       type_cars: String,
                       mileage: Option[Double],
                       color: String,
                       date_of_purchase: String,
                       avg_mileage: Double)

  def countAvgMiles(cars: Dataset[Cars]): Double = {
    val count: (Double, Int) = cars
      .map(car => (car.mileage.getOrElse(0.0), 1))
      .reduce((a, b) => (a._1 + b._1, a._2 + b._2))

    val avgMileage = count._1 / count._2

    avgMileage
  }

  val avgMiles = countAvgMiles(carsDS)

  val ds: Dataset[CarsOuts] = carsDS
    .map(car =>
      CarsOuts(
        car.id,
        car.price,
        car.brand,
        car.type_cars,
        car.mileage,
        car.color,
        car.date_of_purchase,
        avgMiles
      ))

  ds.show()

  spark.stop()
}
