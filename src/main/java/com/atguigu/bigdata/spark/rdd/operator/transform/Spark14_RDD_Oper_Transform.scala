package com.atguigu.bigdata.spark.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

object Spark14_RDD_Oper_Transform {

    def main(args: Array[String]): Unit = {

        val conf = new SparkConf().setMaster("local[*]").setAppName("RDD")
        conf.set("spark.local.dir", "e:/test")
        val sc = new SparkContext(conf)

        // TODO 算子 - 转换 - reduceByKey
        val rdd : RDD[(String, Int)] = sc.makeRDD(
            List(
                ("a", 1),
                ("a", 1),
                ("a", 1)
            )
        )

        // reduceByKey算子的作用，是将相同的key的value分在一个组中，然后进行reduce操作
        // TODO reduceByKey可以实现WordCount ( 2 / 10 o)
        val wordCount: RDD[(String, Int)] = rdd.reduceByKey(_ + _)

        wordCount.collect.foreach(println)



        sc.stop()

    }
}
