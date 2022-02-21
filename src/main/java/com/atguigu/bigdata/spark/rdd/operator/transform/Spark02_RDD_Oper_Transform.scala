package com.atguigu.bigdata.spark.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark02_RDD_Oper_Transform {

    def main(args: Array[String]): Unit = {

        val conf = new SparkConf().setMaster("local[*]").setAppName("RDD")
        val sc = new SparkContext(conf)

        // TODO 算子 - 转换 -
        val rdd = sc.makeRDD(List(1,2,3,4), 2)

        val rdd1 = rdd.mapPartitions(
            list => {
                println("*********************")
                list.map(_*2)
            }
        )

        rdd1.collect().foreach(println)


        sc.stop()

    }
}
