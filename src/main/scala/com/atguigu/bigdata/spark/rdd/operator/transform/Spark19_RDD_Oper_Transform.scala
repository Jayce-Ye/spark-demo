package com.atguigu.bigdata.spark.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark19_RDD_Oper_Transform {

    def main(args: Array[String]): Unit = {

        val conf = new SparkConf().setMaster("local[*]").setAppName("RDD")
        conf.set("spark.local.dir", "e:/test")
        val sc = new SparkContext(conf)

        // TODO 将数据List(("a", 88), ("b", 95), ("a", 91), ("b", 93), ("a", 95), ("b", 98))求每个key的平均值
        val rdd = sc.makeRDD(
            List(
                ("a", 2), ("a", 1), ("c", 3), ("b", 4)
            )
        )

        //  ("a", 1)("a", 2)("b", 4) ("c", 3)
        // sortByKey算子就是按照key排序
        val rdd1: RDD[(String, Int)] = rdd.sortByKey(false)

        rdd1.collect.foreach(println)


        sc.stop()

    }
}
