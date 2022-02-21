package com.atguigu.bigdata.spark.rdd.operator.action

import org.apache.spark.{SparkConf, SparkContext}

object Spark03_RDD_Oper_Action {

    def main(args: Array[String]): Unit = {

        // 一个应用程序, Driver程序
        val conf = new SparkConf().setMaster("local[*]").setAppName("RDD")
        val sc = new SparkContext(conf)

        // TODO 算子 - 行动
        val rdd = sc.makeRDD(List(1,4,3,2),2)

        // aggregate & aggregateByKey的区别？
        // 1. 数据格式
        // 2. aggregateByKey是一个转换算子，所以执行后会产生新的RDD
        //    aggregate是一个行动算子，所以执行后会得到结果
        // 3. aggregateByKey执行计算时，初始值只会参与分区内计算
        //    aggregate执行计算时，初始值会参与分区内计算,也会参与分区间的计算
        //  【1，4】，【3，2】
        //  【5，1，4】，【5，3，2】
        // 【10】【10】
        // 【5， 10， 10】
        val i: Int = rdd.aggregate(5)(_ + _, _ + _)
        val j: Int = rdd.fold(5)(_ + _)
        val k: Int = rdd.reduce(_ + _)

        println(i)

        sc.stop()

    }
}
