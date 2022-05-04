package com.atguigu.bigdata.spark.rdd.operator.action

import org.apache.spark.{SparkConf, SparkContext}

object Spark05_RDD_Oper_Action {

    def main(args: Array[String]): Unit = {

        // 一个应用程序, Driver程序
        val conf = new SparkConf().setMaster("local[*]").setAppName("RDD")
        val sc = new SparkContext(conf)

        // TODO 算子 - 行动
        val rdd = sc.makeRDD(List(
            ("a", 2), ("a", 3)
        ),2)

        rdd.saveAsTextFile("output")
        rdd.saveAsObjectFile("output1")
        rdd.saveAsSequenceFile("output2")



        sc.stop()

    }
}
