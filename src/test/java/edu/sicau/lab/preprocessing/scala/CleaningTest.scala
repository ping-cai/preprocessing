package edu.sicau.lab.preprocessing.scala

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._

class CleaningTest {

}

object CleaningTest {
  def main(args: Array[String]): Unit = {
    //    第一层找顶层抽象，创建spark程序入口
    val sparkSession = SparkSession.builder()
      .appName("CleaningTest")
      .master("local[*]")
      .getOrCreate()
    //    //   第二步，尝试导入文件操作
    //    val dataFrame = sparkSession.read
    //      .csv("G:/Destop/轨道交通项目开发/开发阶段/各类数据/AFC原始数据.txt")
    //    第三步更改schema信息
    val schema = StructType(
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
    //       第四步，从新导入文件操作，包含自动schema信息
    val dataFrame = sparkSession.read
      .schema(schema)
      .csv("G:/Destop/轨道交通项目开发/开发阶段/各类数据/AFC原始数据.txt")
    dataFrame.createOrReplaceTempView("afc")
    //    第五步清除字段前后空格和转换日期格式
    val afcTrimSql = "select trim(id) id,kind,action,stations,beforebanlance,afterbanlance,recivetime from afc"
    val afcTrim = sparkSession.sql(afcTrimSql)
    afcTrim.createOrReplaceTempView("afc_trim")
    sparkSession.udf.register("standardDate", udfDate(_))
    val toStandardDateSql = "select id,kind,action,stations,beforebanlance,afterbanlance,standardDate(recivetime) from afc_trim"
    sparkSession.sql(toStandardDateSql).show()
  }

  def udfDate(dateString: String): String = {
    val dateArray = dateString.split(" ")
    val date = dateArray(0)
    val time = dateArray(1)
    val ymd = date.split("/")
    val year = ymd(0)
    val month = ymd(1)
    val day = ymd(2)
    val isLessThan: String => String = (number: String) => {
      if (number.toInt < 10) {
        val lessThan = String.format("0%s", number)
        lessThan
      } else {
        number
      }
    }
    val monthAdd = isLessThan(month)
    val dayAdd = isLessThan(day)
    val standardDate = String.format("%s-%s-%s %s", year, monthAdd, dayAdd, time)
    standardDate
  }
}