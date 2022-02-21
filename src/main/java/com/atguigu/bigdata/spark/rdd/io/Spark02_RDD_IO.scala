package com.atguigu.bigdata.spark.rdd.io

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark02_RDD_IO {

    def main(args: Array[String]): Unit = {

        val conf = new SparkConf().setMaster("local[*]").setAppName("WordCount")
        val sc = new SparkContext(conf)

        //val rdd1: RDD[String] = sc.textFile("output")
        //rdd1.collect().foreach(println)

        //val rdd1 = sc.objectFile[(String, Int)]("output1")
        //rdd1.collect().foreach(println)

        val rdd2 = sc.sequenceFile[String, Int]("output2")
        rdd2.collect().foreach(println)

        sc.stop()

    }
}
