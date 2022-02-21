package com.atguigu.bigdata.spark.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark01_RDD_Oper_Transform_1 {

    def main(args: Array[String]): Unit = {

        val conf = new SparkConf().setMaster("local[*]").setAppName("RDD")
        val sc = new SparkContext(conf)

        // TODO 算子 - 转换 - map

        // 【1，2】，【3，4】
        val rdd = sc.makeRDD(List(1,2,3,4), 2)

        // TODO 分区 - 2
        // 数据执行顺序
        val rdd1: RDD[Int] = rdd.map(
            num => {
                println("********* num = " + num)
                num
            }
        )

        val rdd2: RDD[Int] = rdd1.map(
            num => {
                println("######## num = " + num)
                num
            }
        )

        rdd2.saveAsTextFile("output")


        sc.stop()

    }
}
