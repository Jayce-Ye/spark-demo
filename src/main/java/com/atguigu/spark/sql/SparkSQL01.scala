package com.atguigu.spark.sql

import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

object SparkSQL01 {
  def main(args: Array[String]): Unit = {
    val sparkSession: SparkSession = SparkSession.builder()
      .master("local[2]")
      .appName("SparkSQL")
      .getOrCreate()
    import sparkSession.implicits._

    val dataFrame: DataFrame = sparkSession.read.json("data/user.json")


//    dataFrame.show()

    //1. SQL语法
//    dataFrame.createTempView("user");
//
//    sparkSession.sql("select * from user").show()

    //2. DLS 语法
//    dataFrame.select($"name").show

//    dataFrame.groupBy($"name").count().show()

    //3. rdd, dataFrame, dataSet相互转换
    val dataSet: Dataset[User] = dataFrame.as[User]
//    val rdd: RDD[User] = dataSet.rdd
//
//    val rdd1: RDD[Row] = dataFrame.rdd

    //4. DataSet编程
//    dataSet.join(dataSet, "name").show

    val dataSet2: Dataset[Long] = dataSet.map(_.age)

    println(dataSet2.reduce(_ + _))
    sparkSession.close()
  }
}


case class User(name: String, age: Long)
