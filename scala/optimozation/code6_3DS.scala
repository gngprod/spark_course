//package optimozation
//
//import org.apache.spark.sql.functions._
//import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
//
//object code6_3DS extends App {
//  val spark = SparkSession.builder()
//    .appName("sparkLearn")
//    .master("local[*]")
//    .getOrCreate()
//
//  // Это делается для того, чтобы небольшие данные
//  // обрабатывались так же, как если бы работа шла с большими данными.
//  spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")
//
//  // Сгенерируем данные:
//
//  import spark.implicits._
//
//  case class NameColumn(
//                         id: Int,
//                         salary: Int)
//
//  case class NameColumnOut(
//                            id: String,
//                            salary: String,
//                            col_0: String,
//                            col_1: String,
//                            col_2: String,
//                            col_3: String,
//                            col_4: String,
//                            col_5: String,
//                            col_6: String,
//                            col_7: String,
//                            col_8: String,
//                            col_9: String)
//
//  val data1 = (1 to 500000).map(i => (i, i * 100))
//  val data2 = (1 to 10000).map(i => (i, i * 1000))
//  val df1 = data1.toDF("id", "salary").repartition(5).as[NameColumn]
//  val df2 = data2.toDF("id", "salary").repartition(10).as[NameColumn]
//
//  // В процессе работы с датафреймами понадобится
//  // добавить определенное количество колонок, опишем это действие функцией:
//  def addColumn(ds: Dataset[NameColumn], n: Int): Dataset[NameColumn] = {
//    val columns = (1 to n).map(col => s"col_$col")
//    columns.foldLeft(ds)((ds, column) => ds.withColumn(column, lit("n/a")).as[NameColumn])
//  }
//
//  //Способ 1.1 - добавим колонки и объединим партицированные датафреймы
//  val dfWithColumns11: Dataset[NameColumn] = df2.transform(addColumn(df2, 10))
////    .map(ord => NameColumnOut(ord.id,
////      ord.id,
////      ord.salary,
////      ord.col_0,
////      ord.col_1,
////      ord.col_2,
////      ord.col_3,
////      ord.col_4,
////      ord.col_5,
////      ord.col_6,
////      ord.col_7,
////      ord.col_8))
//  val joinedDF11 = dfWithColumns11.join(df1, "id").as[NameColumnOut]
//
////  joinedDF11.show()
////  joinedDF11.explain()
//
////  //Способ 1.2 - объединим партицированные датафреймы и добавим колонки
////  val joinedDF12 = df2.join(df1, "id")
////  val dfWithColumns12 = addColumn(joinedDF12, 10)
////  dfWithColumns12.show()
////  dfWithColumns12.explain()
//
////  //Способ 2.1 - заново разделим данные, но на этот раз разделять будем по id; затем объединим данные и добавим колонки:
////  val repartitionedById11 = df1.repartition(col("id"))
////  val repartitionedById21 = df2.repartition(col("id"))
////  val joinedDF21 = repartitionedById21.join(repartitionedById11, "id")
////  val dfWithColumns21 = addColumn(joinedDF21, 10)
////  dfWithColumns21.show()
////  dfWithColumns21.explain()
////
////  //Способ 2.2 делим по id, добавляем колонки и объединяем данные
////  val repartitionedById12 = df1.repartition(col("id"))
////  val repartitionedById22 = df2.repartition(col("id"))
////  val dfWithColumns22 = addColumn(repartitionedById12, 10)
////  val joinedDF22 = repartitionedById22.join(dfWithColumns22, "id")
////  joinedDF22.show()
////  joinedDF22.explain()
//
//  System.in.read()
//  spark.stop()
//}
//
//// 1.1
////== Physical Plan ==
////AdaptiveSparkPlan isFinalPlan=false
////+- Project [id#23, salary#24, n/a AS col_1#32, n/a AS col_2#41, n/a AS col_3#51, n/a AS col_4#62, n/a AS col_5#74, n/a AS col_6#87, n/a AS col_7#101, n/a AS col_8#116, n/a AS col_9#132, n/a AS col_10#149, salary#8]
////   +- SortMergeJoin [id#23], [id#7], Inner
////      :- Sort [id#23 ASC NULLS FIRST], false, 0
////      :  +- Exchange hashpartitioning(id#23, 200), ENSURE_REQUIREMENTS, [id=#181]
////      :     +- Exchange RoundRobinPartitioning(10), REPARTITION_BY_NUM, [id=#174]
////      :        +- LocalTableScan [id#23, salary#24]
////      +- Sort [id#7 ASC NULLS FIRST], false, 0
////         +- Exchange hashpartitioning(id#7, 200), ENSURE_REQUIREMENTS, [id=#182]
////            +- Exchange RoundRobinPartitioning(5), REPARTITION_BY_NUM, [id=#176]
////               +- LocalTableScan [id#7, salary#8]