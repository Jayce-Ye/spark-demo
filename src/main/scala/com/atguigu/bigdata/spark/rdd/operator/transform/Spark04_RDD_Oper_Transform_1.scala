package com.atguigu.bigdata.spark.rdd.operator.transform

import org.apache.spark.{SparkConf, SparkContext}

object Spark04_RDD_Oper_Transform_1 {

    def main(args: Array[String]): Unit = {

        val conf = new SparkConf().setMaster("local[*]").setAppName("RDD")
        val sc = new SparkContext(conf)

        // TODO 算子 - 转换 - 扁平化
        val rdd = sc.makeRDD(
            List(List(1,2),3,List(4,5))
        )

        val rdd1 = rdd.flatMap {
            case list : List[_] => list
            case other => List(other)
        }

        rdd1.collect.foreach(println)


        sc.stop()

    }
}
