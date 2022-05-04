package com.atguigu.bigdata.spark.rdd.acc

import org.apache.spark.{SparkConf, SparkContext}

object Spark02_Acc {

    def main(args: Array[String]): Unit = {

        val conf = new SparkConf().setMaster("local[*]").setAppName("WordCount")
        val sc = new SparkContext(conf)

        val rdd = sc.makeRDD(
            List(1,2,3,4),2
        )

        // TODO 创建累加器
        val sum = sc.longAccumulator("sum")
        //sc.collectionAccumulator()

        val rdd1 = rdd.map(
            num => {
                // 使用累计器
                sum.add(num)
                num * 2
            }
        )
//        rdd1.collect
//        rdd1.foreach(println)
        println("***********************")

        // TODO 累计器重复计算的问题。
        // TODO 累计器没有计算的问题。
        // 获取累加器的结果
        println(sum.value);

        sc.stop()

    }
}
