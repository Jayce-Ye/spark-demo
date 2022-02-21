package com.atguigu.spark.sql

import org.apache.spark.sql.SparkSession

object SparkSQL06_Hive {
  def main(args: Array[String]): Unit = {
    val sparkSession: SparkSession = SparkSession.builder()
      .master("local[2]")
      .enableHiveSupport()
      .appName("SparkSQL")
      .getOrCreate()

    //读取Hive数据
    sparkSession.sql("select * from city_info")

    sparkSession.close()
  }

}
