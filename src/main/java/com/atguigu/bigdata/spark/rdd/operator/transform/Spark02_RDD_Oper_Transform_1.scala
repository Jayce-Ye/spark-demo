package com.atguigu.bigdata.spark.rdd.operator.transform

import org.apache.spark.{SparkConf, SparkContext}

object Spark02_RDD_Oper_Transform_1 {

    def main(args: Array[String]): Unit = {

        val conf = new SparkConf().setMaster("local[*]").setAppName("RDD")
        val sc = new SparkContext(conf)

        // TODO 算子 - 转换 -
        val rdd = sc.makeRDD(List(1,2,3,4), 2)
        // 获取每个数据分区的最大值
        // 【1，2】【3，4】
        val rdd1 = rdd.mapPartitions(
            list => {
                val max = list.max
                List(max).iterator
            }
        )
        rdd1.collect.foreach(println)



        sc.stop()

    }
}
