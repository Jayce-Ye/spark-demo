package com.atguigu.bigdata.spark.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark10_RDD_Oper_Transform_1 {

    def main(args: Array[String]): Unit = {

        val conf = new SparkConf().setMaster("local[*]").setAppName("RDD")
        conf.set("spark.local.dir", "e:/test")
        val sc = new SparkContext(conf)

        // TODO 算子 - 转换 - 缩减分区
        val rdd : RDD[Int] = sc.makeRDD(
            List(1,2,3,4,5,6), 2
        )

        // 扩大分区 - repartition
        // 在不shuffle的情况下，coalesce算子扩大分区是没有意义的。
        //val rdd1: RDD[Int] = rdd.coalesce(3, true)

        val rdd1: RDD[Int] = rdd.repartition(3)


        rdd.saveAsTextFile("output")
        rdd1.saveAsTextFile("output1")




        sc.stop()

    }
}
