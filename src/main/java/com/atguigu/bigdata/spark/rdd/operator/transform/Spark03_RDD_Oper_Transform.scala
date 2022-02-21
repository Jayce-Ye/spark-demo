package com.atguigu.bigdata.spark.rdd.operator.transform

import org.apache.spark.{SparkConf, SparkContext}

object Spark03_RDD_Oper_Transform {

    def main(args: Array[String]): Unit = {

        val conf = new SparkConf().setMaster("local[*]").setAppName("RDD")
        val sc = new SparkContext(conf)

        // TODO 算子 - 转换 -
        val rdd = sc.makeRDD(List(1,2,3,4,5,6), 3)
        // 【1，2】，【3，4】，【5，6】
//        var cnt = 0
//        val rdd1 = rdd.mapPartitions(
//            list => {
//                if ( cnt == 1 ) {
//                    cnt = cnt + 1
//                    list
//                } else {
//                    cnt = cnt + 1
//                    Nil.iterator
//                }
//            }
//        )
//        rdd1.collect().foreach(println)
        val rdd1 = rdd.mapPartitionsWithIndex(
            (ind, list) => {
                if ( ind == 1 ) {
                    list
                } else {
                    Nil.iterator
                }
            }
        )
        rdd1.collect().foreach(println)



        sc.stop()

    }
}
