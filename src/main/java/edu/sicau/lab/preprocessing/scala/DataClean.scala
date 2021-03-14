package edu.sicau.lab.preprocessing.scala

import edu.sicau.lab.preprocessing.scala.util.AFCDateClean
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.log4j.{Level, Logger}

class DataClean {

}

object DataClean {

  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession
      .builder()
      .appName("DataClean")
      .master("local[*]")
      .getOrCreate()

    val schema: StructType = StructType(
      Array(
        StructField("id", StringType, nullable = true),
        StructField("days", IntegerType, nullable = true),
        StructField("shijian", IntegerType, nullable = true),
        StructField("kind", StringType, nullable = true),
        StructField("action", StringType, nullable = true),
        StructField("stations", IntegerType, nullable = true),
        StructField("beforebanlance", DoubleType, nullable = true),
        StructField("money", DoubleType, nullable = true),
        StructField("afterbanlance", DoubleType, nullable = true),
        StructField("liancheng", StringType, nullable = true),
        StructField("counter", IntegerType, nullable = true),
        StructField("trainid", IntegerType, nullable = true),
        StructField("recivetime", StringType, nullable = true)
      )
    )
    val dataFrame = sparkSession.read
      .schema(schema)
      .option("sep", ",")
      .option("inferSchema", "true")
      .csv("G:/Destop/轨道交通项目开发/开发阶段/各类数据/AFC原始数据.txt")
    dataFrame.createOrReplaceTempView("afc")
    val aFCDateClean = new AFCDateClean
    sparkSession.udf.register("standardDateConversion", aFCDateClean.standardDateConversion(_))
    val cleanSql = "select trim(id) id,kind,action,stations,beforebanlance,money,afterbanlance,standardDateConversion(recivetime) recivetime from afc"
    val cleanedData = sparkSession.sql(cleanSql)
    cleanedData.createOrReplaceTempView("afc_cleaned")
    val year = 2018
    val month = 9
    val inStation = s"select * from afc_cleaned where action='进站' and year(recivetime)=$year and month(recivetime) = $month"
    val outStation = s"select * from afc_cleaned where action='出站' and year(recivetime)=$year and month(recivetime) = $month"
    val inStationData = sparkSession.sql(inStation)
    inStationData.createOrReplaceTempView("in_station")
    val outStationData = sparkSession.sql(outStation)
    outStationData.createOrReplaceTempView("out_station")
    val stationMatchedToODSql =
      "select od.*,row_number() over(partition by id,kind,in_number,in_time sort by id) as row_num from " +
        "(select in_station.id,in_station.kind,in_station.stations in_number,in_station.recivetime in_time," +
        "out_station.stations out_number,out_station.recivetime out_time " +
        "from in_station join out_station " +
        "on in_station.id=out_station.id and in_station.recivetime<out_station.recivetime) as od "
    val stationMatchedToODSql2 =
      "select * from " +
        "(select in_station.id,in_station.kind,in_station.stations in_number,in_station.recivetime in_time," +
        "out_station.stations out_number,out_station.recivetime out_time," +
        "row_number() over(partition by in_station.id,in_station.kind,in_station.stations,in_station.recivetime sort by in_station.id) as row_num " +
        "from in_station join out_station " +
        "on in_station.id=out_station.id and in_station.recivetime<out_station.recivetime) as od "
    sparkSession.sql(stationMatchedToODSql2).show()
  }

}
