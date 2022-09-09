package optimozation

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

object code6_3 extends App {
  val spark = SparkSession.builder()
    .appName("sparkLearn")
    .master("local[*]")
    .getOrCreate()

  // Это делается для того, чтобы небольшие данные
  // обрабатывались так же, как если бы работа шла с большими данными.
  spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")

  // В процессе работы с датафреймами понадобится
  // добавить определенное количество колонок, опишем это действие функцией:
  def addColumn(df: DataFrame, n: Int): DataFrame = {
    val columns = (1 to n).map(col => s"col_$col")
    columns.foldLeft(df)((df, column) => df.withColumn(column, lit("n/a")))
  }

  // Сгенерируем данные:
  import spark.implicits._
  val data1 = (1 to 500000).map(i => (i, i * 100))
  val data2 = (1 to 10000).map(i => (i, i * 1000))
  val df1 = data1.toDF("id","salary").repartition(5)
  val df2 = data2.toDF("id","salary").repartition(10)


//  //Способ 1.1 - добавим колонки и объединим партицированные датафреймы
//  val dfWithColumns11 = addColumn(df2, 10)
//  val joinedDF11 = dfWithColumns11.join(df1, "id")
//  joinedDF11.show()
//  joinedDF11.explain()
//
//  //Способ 1.2 - объединим партицированные датафреймы и добавим колонки
//  val joinedDF12 = df2.join(df1, "id")
//  val dfWithColumns12 = addColumn(joinedDF12, 10)
//  dfWithColumns12.show()
//  dfWithColumns12.explain()
//
//  //Способ 2.1 - заново разделим данные, но на этот раз разделять будем по id; затем объединим данные и добавим колонки:
//  val repartitionedById11 = df1.repartition(col("id"))
//  val repartitionedById21 = df2.repartition(col("id"))
//  val joinedDF21 = repartitionedById21.join(repartitionedById11, "id")
//  val dfWithColumns21 = addColumn(joinedDF21, 10)
//  dfWithColumns21.show()
//  dfWithColumns21.explain()
//
//  //Способ 2.2 делим по id, добавляем колонки и объединяем данные
//  val repartitionedById12 = df1.repartition(col("id"))
//  val repartitionedById22 = df2.repartition(col("id"))
//  val dfWithColumns22 = addColumn(repartitionedById12, 10)
//  val joinedDF22 = repartitionedById22.join(dfWithColumns22, "id")
//  joinedDF22.show()
//  joinedDF22.explain()

  //Способ 3 делим по id, добавляем колонки, удаляем символы, джоиним, делаем distinct, фильтруем по salary
  val repartitionedById13 = df1.repartition(col("id"))
  val repartitionedById23 = df2.repartition(col("id"))
  val dfWithColumns23 = addColumn(repartitionedById13, 10)
  val regDfWithColumns23 = dfWithColumns23.withColumn("col_1",
    regexp_replace(col("col_1"), "n", "").as("col_1"))
  val joinedDF23 = repartitionedById23.join(regDfWithColumns23, "id")
  val distJoinedDF23 = joinedDF23.distinct()
  val filtDistJoinedDF23 = distJoinedDF23.filter(repartitionedById23.col("salary") === 5087000)
  filtDistJoinedDF23.show()
  filtDistJoinedDF23.explain()

  System.in.read()
  spark.stop()
}
