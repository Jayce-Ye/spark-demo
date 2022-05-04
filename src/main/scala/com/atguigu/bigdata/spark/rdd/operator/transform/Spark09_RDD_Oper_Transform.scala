package com.atguigu.bigdata.spark.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark09_RDD_Oper_Transform {

    def main(args: Array[String]): Unit = {

        val conf = new SparkConf().setMaster("local[*]").setAppName("RDD")
        conf.set("spark.local.dir", "e:/test")
        val sc = new SparkContext(conf)

        // TODO 算子 - 转换
        val rdd : RDD[Int] = sc.makeRDD(
            List(1,1,1)
        )

        // map(x => (x, null)).reduceByKey((x, _) => x, numPartitions).map(_._1)
        // 【1，1，1】
        // 【(1, null)，(1, null)，(1, null)】
        // 【null, null, null】
        // 【null, null】
        // 【(1, null)】
        // 【1】
        val rdd1: RDD[Int] = rdd.distinct()
        rdd1.collect.foreach(println)

        //List(1,1,1,1,1).distinct



        sc.stop()

    }
}
