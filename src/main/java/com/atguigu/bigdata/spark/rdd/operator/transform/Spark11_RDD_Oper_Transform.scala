package com.atguigu.bigdata.spark.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark11_RDD_Oper_Transform {

    def main(args: Array[String]): Unit = {

        val conf = new SparkConf().setMaster("local[*]").setAppName("RDD")
        conf.set("spark.local.dir", "e:/test")
        val sc = new SparkContext(conf)

        // TODO 算子 - 转换 - 排序
        val rdd : RDD[Int] = sc.makeRDD(
            List(1,4,3,2,6,5),2
        )

        val rdd1: RDD[Int] = rdd.sortBy(num => num, false)
        println(rdd1.collect.mkString(","))





        sc.stop()

    }
}
