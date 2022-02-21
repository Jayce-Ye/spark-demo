package com.atguigu.bigdata.spark.rdd.operator.action

import org.apache.spark.{SparkConf, SparkContext}

object Spark06_RDD_Oper_Action {

    def main(args: Array[String]): Unit = {

        // 一个应用程序, Driver程序
        val conf = new SparkConf().setMaster("local[*]").setAppName("RDD")
        val sc = new SparkContext(conf)

        // TODO 算子 - 行动
        val rdd = sc.makeRDD(
            List(1,4,3,2),2
        )

        // collect是按照分区号码进行采集
        rdd.collect.foreach(println)
        println("****************************")
        rdd.foreach(println)



        sc.stop()

    }
}
