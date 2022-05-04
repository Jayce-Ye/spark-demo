package com.atguigu.bigdata.spark.rdd.operator.action

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark01_RDD_Oper_Action {

    def main(args: Array[String]): Unit = {

        // 一个应用程序, Driver程序
        val conf = new SparkConf().setMaster("local[*]").setAppName("RDD")
        val sc = new SparkContext(conf)

        // TODO 算子 - 行动
        val rdd = sc.makeRDD(List(1,2,3,4))

        // Spark RDD方法分为2大类，其中一个是转换算子，一个为行动算子
        // 行动算子在被调用时，会触发Spark作业的执行
        // collect算子就是行动算子
        // 行动算子执行时，会构建新的作业
        rdd.collect() // new job
        //rdd.collect() // new job


        sc.stop()

    }
}
