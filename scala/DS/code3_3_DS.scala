package DS

import org.apache.spark.sql.functions.{col, collect_list, concat_ws, split}
import org.apache.spark.sql.{Dataset, SparkSession}

object code3_3_DS extends App {
  val spark = SparkSession.builder()
    .appName("sparkLearn")
    .master("local[*]")
    .getOrCreate()

  import spark.implicits._

  case class AiJobsIndustry(
                             JobTitle: String,
                             Company: String,
                             Location: String,
                             CompanyReviews: String)

  val AiJobsIndustryDS: Dataset[AiJobsIndustry] = spark.read
    .option("multiLine", "true")
    .option("header", "true")
    .option("inferSchema", "true")
    .csv(raw"src\main\resources\AiJobsIndustry.csv")
    .na.drop()
    .drop("Link")
    .withColumn("Company", split(col("Company"), raw"\n")(1))
    .as[AiJobsIndustry]

  def extractIntegerValueDS(ds: Dataset[AiJobsIndustry]): Dataset[AiJobsIndustryNew] = {
    ds.map(x => AiJobsIndustryNew(
      x.JobTitle,
      x.Company,
      x.Location,
      x.CompanyReviews.replaceAll("\\D", "").toInt
    ))
  }


  def createDSOut(ds: Dataset[AiJobsIndustryNew]): Dataset[AiJobsIndustryOut] = {
    def createRaw(ds: Dataset[AiJobsIndustryNew], stType: String, ctType: String): Dataset[AiJobsIndustryOut] = {
      val sumDS: Dataset[Sum] = ds
        .groupByKey(if (stType == "Company") _.Company else _.JobTitle)
        .mapGroups { (id, orders) =>
          val name = id
          val count = orders.map(order => order.CompanyReviews).sum
          Sum(name, count)
        }

      val agrSumDS = if (ctType == "max") {
        sumDS.orderBy(col("count").desc).limit(1)
      } else {
        sumDS.orderBy(col("count")).limit(1)
      }

      val locationDS: Dataset[Locat] = ds
        .map(ord => Locat(if (stType == "Company") ord.Company else ord.JobTitle, ord.Location)).distinct()
        .groupBy("name")
        .agg(concat_ws(", ", collect_list("location")).as("location")).as[Locat]

      val outDS: Dataset[AiJobsIndustryOut] =
        agrSumDS.joinWith(locationDS, agrSumDS.col("name") === locationDS.col("name"))
          .map(record =>
            AiJobsIndustryOut(
              record._1.name,
              status_type = stType,
              record._2.location,
              record._1.count,
              count_type = ctType
            ))
      outDS
    }

    val companyMax = createRaw(ds, "Company", "max")
    val companyMin = createRaw(ds, "Company", "min")
    val jtMax = createRaw(ds, "JobTitle", "max")
    val jtMin = createRaw(ds, "JobTitle", "min")
    companyMax.union(companyMin).union(jtMax).union(jtMin)
  }

  case class AiJobsIndustryNew(
                                JobTitle: String,
                                Company: String,
                                Location: String,
                                CompanyReviews: Int)

  case class Sum(
                  name: String,
                  count: Int)

  case class Locat(
                    name: String,
                    location: String)

  case class AiJobsIndustryOut(
                                name: String,
                                status_type: String,
                                location: String,
                                count: Int,
                                count_type: String)

  val AiJobsIndustryDSOut = AiJobsIndustryDS
    .transform(extractIntegerValueDS)
    .transform(createDSOut)
  AiJobsIndustryDSOut.show()

  spark.stop()
}
