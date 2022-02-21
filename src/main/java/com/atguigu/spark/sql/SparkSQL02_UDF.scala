package com.atguigu.spark.sql

import org.apache.spark.sql.{DataFrame, SparkSession}

object SparkSQL02_UDF {
  def main(args: Array[String]): Unit = {
    val sparkSession: SparkSession = SparkSession.builder()
      .master("local[2]")
      .appName("SparkSQL")
      .getOrCreate()

    val df: DataFrame = sparkSession.read.json("data/user.json")

    df.createTempView("user")

    //如何自定义UDF函数
    sparkSession.udf.register("addpre", (x: String, y: String) => y + x)

    sparkSession.sql("select addpre(name, 'mingzi:') from user").show

    sparkSession.close()
  }
}
