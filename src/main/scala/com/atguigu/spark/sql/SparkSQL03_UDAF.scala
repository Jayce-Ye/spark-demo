package com.atguigu.spark.sql

import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.sql._

object SparkSQL03_UDAF {
  def main(args: Array[String]): Unit = {
    val sparkSession: SparkSession = SparkSession.builder()
      .master("local[2]")
      .appName("SparkSQL")
      .getOrCreate()

    val df: DataFrame = sparkSession.read.json("data/user.json")

    df.createTempView("user")

    //如何自定义UDAF函数
    sparkSession.udf.register("my_avg", functions.udaf(new MyUDAF))

    sparkSession.sql("select my_avg(age) from user").show

    sparkSession.close()
  }
}

case class MyBuf(var total: Long, var count: Int)

class MyUDAF extends Aggregator[Long, MyBuf, Double] {
  override def zero: MyBuf = MyBuf(0, 0)

  override def reduce(b: MyBuf, a: Long): MyBuf = {
    b.total += a
    b.count += 1
    b
  }

  override def merge(b1: MyBuf, b2: MyBuf): MyBuf = {
    b1.total += b2.total
    b1.count += b2.count
    b1
  }

  override def finish(r: MyBuf): Double = r.total.toDouble / r.count

  override def bufferEncoder: Encoder[MyBuf] = Encoders.product

  override def outputEncoder: Encoder[Double] = Encoders.scalaDouble
}