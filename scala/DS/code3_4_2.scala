package DS

import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{Dataset, SparkSession}

object code3_4_2 extends App {
  val spark = SparkSession.builder()
    .appName("sparkLearn")
    .master("local[*]")
    .getOrCreate()

  import spark.implicits._

  case class Hrdataset(
                        PositionID: Int,
                        Position: String)

  val hrdatasetDS: Dataset[Hrdataset] = spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv(raw"src\main\resources\hrdataset.csv").as[Hrdataset]
    .map(ord => Hrdataset(ord.PositionID, ord.Position))
  hrdatasetDS.show()

  def extractDS(ds: Dataset[extr]): Dataset[Hrdataset] = {
    val hrdatasetDSheads = hrdatasetDS
      .map(ord => Hrdataset(ord.PositionID, ord.Position.split(" ").head.toLowerCase()))
      .joinWith(ds, col("Position") === ds.col("PositionHead"))
      .map(ord => Hrdataset(ord._1.PositionID, ord._1.Position))
      .distinct()
    hrdatasetDSheads.joinWith(hrdatasetDS,
      hrdatasetDSheads.col("PositionID") === hrdatasetDS.col("PositionID"))
      .map(ord => Hrdataset(ord._2.PositionID, ord._2.Position))
      .distinct()
      .orderBy("PositionID")
  }

  def inputValue(value: List[String]): Unit = {
    val valueLow = value.map(_.toLowerCase())
    val extrDS: Dataset[extr] = valueLow.toDF("PositionHead").as[extr]
    extractDS(extrDS).show()
  }

  case class extr(PositionHead: String)

  inputValue(List("BI", "it"))
  spark.stop()

}
