package com.atguigu.bigdata.spark.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark10_RDD_Oper_Transform {

    def main(args: Array[String]): Unit = {

        val conf = new SparkConf().setMaster("local[*]").setAppName("RDD")
        conf.set("spark.local.dir", "e:/test")
        val sc = new SparkContext(conf)

        // TODO 算子 - 转换 - 缩减分区
        val rdd : RDD[Int] = sc.makeRDD(
            List(1,2,3,4,5,6), 3
        )

        // 缩减 (合并)， 默认情况下，缩减分区不会shuffle
        //val rdd1: RDD[Int] = rdd.coalesce(2)
        // 这种方式在某些情况下，无法解决数据倾斜问题，所以还可以在缩减分区的同时，进行数据的shuffle操作
        val rdd2: RDD[Int] = rdd.coalesce(2, true)

        rdd.saveAsTextFile("output")
        rdd2.saveAsTextFile("output1")




        sc.stop()

    }
}
