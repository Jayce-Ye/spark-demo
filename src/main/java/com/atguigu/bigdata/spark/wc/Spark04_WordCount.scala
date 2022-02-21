package com.atguigu.bigdata.spark.wc

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark04_WordCount {

    def main(args: Array[String]): Unit = {

        // TODO 使用Spark
        val conf = new SparkConf().setMaster("local[*]").setAppName("WordCount")
        val sc = new SparkContext(conf)
        val wordCount : RDD[(String, Int)] = sc.textFile("data/word.txt").map((_,1)).reduceByKey(_ + _)
        val array: Array[(String, Int)] = wordCount.collect()

        // foreach方法其实是Scala集合（单点）的方法
        //array.foreach(println)

        // foreach方法其实是RDD（分布式）的方法
        //wordCount.foreach(println)

        sc.stop()

    }
}
