package com.atguigu.bigdata.spark.rdd.operator.action

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark04_RDD_Oper_Action {

    def main(args: Array[String]): Unit = {

        // 一个应用程序, Driver程序
        val conf = new SparkConf().setMaster("local[*]").setAppName("RDD")
        val sc = new SparkContext(conf)

        // TODO 算子 - 行动
        val rdd = sc.makeRDD(List(1,4,3,2),2)

        // countByKey算子表示相同key出现的次数
        val rdd1: RDD[(String, Int)] = rdd.map(("a", _))
        // (a, 1), (a, 4), (a, 3), (a, 2)
        // (a, 4) => (a, 1),(a, 1),(a, 1),(a, 1)
        // (a, 10)

        // TODO countByKey算子可以实现 WordCount (7 / 10)
        val map: collection.Map[String, Long] = rdd1.countByKey()
        println(map)


        sc.stop()

    }
}
