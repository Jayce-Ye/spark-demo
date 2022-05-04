package com.atguigu.bigdata.spark.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark12_RDD_Oper_Transform {

    def main(args: Array[String]): Unit = {

        val conf = new SparkConf().setMaster("local[*]").setAppName("RDD")
        conf.set("spark.local.dir", "e:/test")
        val sc = new SparkContext(conf)

        // TODO 算子 - 转换 - 排序
        val rdd : RDD[Int] = sc.makeRDD(
            List(1,2,3,4),2
        )

        val rdd1 : RDD[Int] = sc.makeRDD(
            List(3,4,5,6),2
        )
        val rdd2 : RDD[String] = sc.makeRDD(
            List("3","4","5", "6"),2
        )
        // 交集
        //println(rdd.intersection(rdd1).collect().mkString(","))
        // 并集
        //println(rdd.union(rdd1).collect().mkString(","))
        // 差集
        //println(rdd.subtract(rdd1).collect().mkString(","))

        // 拉链
        // 英文翻译：
        // Can only zip RDDs with same number of elements in each partition
        // Can't zip RDDs with unequal numbers of partitions: List(2, 3)
        //println(rdd.zip(rdd1).collect().mkString(","))
        println(rdd.zip(rdd2).collect().mkString(","))





        sc.stop()

    }
}
