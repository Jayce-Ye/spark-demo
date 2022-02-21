package com.atguigu.bigdata.spark.rdd.operator.action

import org.apache.spark.{SparkConf, SparkContext}

object Spark02_RDD_Oper_Action {

    def main(args: Array[String]): Unit = {

        // 一个应用程序, Driver程序
        val conf = new SparkConf().setMaster("local[*]").setAppName("RDD")
        val sc = new SparkContext(conf)

        // TODO 算子 - 行动
        val rdd = sc.makeRDD(List(1,4,3,2),2)

        // reduce算子
        val i: Int = rdd.reduce(_ + _)
        println(i)

        // 将数据从Executor端采集回到Driver端
        // collect会将数据全部拉取到Driver端的内存中，形成数据集合，可能会导致内存溢出
        val ints: Array[Int] = rdd.collect()
        println(ints.mkString(","))

        val l: Long = rdd.count()
        println(l)

        val i1: Int = rdd.first()
        println(i1)

        val ints1: Array[Int] = rdd.take(3)
        println(ints1.mkString(","))

        // 【1，2，3】
        val ints2: Array[Int] = rdd.takeOrdered(3)
        println(ints2.mkString(","))

        sc.stop()

    }
}
