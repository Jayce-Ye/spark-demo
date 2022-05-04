package com.atguigu.spark.sql

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

object SparkSQL05_Write {
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


    frame.createTempView("user")

    val frame2: DataFrame = sparkSession.sql("select name, age+10 as age from user")

    //如果想向MySQL数据库中插入数据
    frame2.write
      .format("jdbc")
      .options(
        Map(
          "url" -> "jdbc:mysql://localhost:3306/test",
          "user" -> "root",
          "useSSL" -> "false",
          "charset" -> "utf8",
          "password" -> "123",
          "dbtable" -> "user",
          "driver" -> "com.mysql.jdbc.Driver"
        )
      )
      .mode(SaveMode.Append)
      .save()

    sparkSession.close()
  }

}
