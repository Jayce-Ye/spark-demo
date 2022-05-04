package com.atguigu.spark.sql

import org.apache.spark.sql.{DataFrame, SparkSession}

object SparkSQL04_Read {
  def main(args: Array[String]): Unit = {
    val sparkSession: SparkSession = SparkSession.builder()
      .master("local[2]")
      .appName("SparkSQL")
      .getOrCreate()

    //    val frame: DataFrame = sparkSession.read
    //      .format("json")
    //      .load("user.json")

    //读取JDBC数据库

    val frame: DataFrame = sparkSession.read
      .format("jdbc")
      .options(Map(
        "url" -> "jdbc:mysql://localhost:3306/test",
        "user" -> "root",
        "useSSL" -> "false",
        "charset" -> "utf8",
        "password" -> "123",
        "dbtable" -> "user",
        "driver" -> "com.mysql.jdbc.Driver"
      ))
      .load()

    frame.show


    sparkSession.close()
  }

}
