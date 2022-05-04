package com.atguigu.bigdata.spark.rdd.operator.action

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark04_RDD_Oper_Action_1 {

    def main(args: Array[String]): Unit = {

        // 一个应用程序, Driver程序
        val conf = new SparkConf().setMaster("local[*]").setAppName("RDD")
        val sc = new SparkContext(conf)

        // TODO 算子 - 行动
        //val rdd = sc.makeRDD(List(1,1,1,1,2,2,3),2)
        val rdd = sc.makeRDD(List(
            ("a", 2), ("a", 3)
        ),2)

        // countByKey算子表示相同key出现的次数
        //val rdd1: RDD[(String, Int)] = rdd.map(("a", _))

        // countByValue中Value不是KV键值对中的v的意思
        // 单value，双value，K-V
        // TODO countByValue可以实现 WordCount (8 / 10)

        // ("a", 2) => "a", "a"
        // ("a", 3) => "a", "a", "a"

        // ( a, 5 )
        val map = rdd.countByValue()

        println(map)


        sc.stop()

    }
}
