package com.atguigu.bigdata.spark.rdd.operator.transform

import org.apache.spark.{SparkConf, SparkContext}

object Spark04_RDD_Oper_Transform {

    def main(args: Array[String]): Unit = {

        val conf = new SparkConf().setMaster("local[*]").setAppName("RDD")
        val sc = new SparkContext(conf)

        // TODO 算子 - 转换 - 扁平化
        val rdd = sc.makeRDD(
            List(
                "Hello Scala", "Hello Spark"
            )
        )
        val rdd1 = sc.makeRDD(
            List(
                List(1,2), List(3,4)
            )
        )

        // 整体 => 个体
        //val rdd2 = rdd.flatMap(_.split(" "))
        val rdd2 = rdd.flatMap(
            str => { // 整体（1）
                // 容器（个体（N））
                str.split(" ")
            }
        )

        val rdd3 = rdd1.flatMap(
            list => {
                list
            }
        )

        rdd3.collect().foreach(println)


        sc.stop()

    }
}
