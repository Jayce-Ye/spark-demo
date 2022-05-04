package com.atguigu.bigdata.spark.wc

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark01_WordCount_Env {

    def main(args: Array[String]): Unit = {

        // TODO 使用Spark
        // Spark是一个计算【框架】。
        // 1. 能找到他 ：增加依赖
        // 2. 获取Spark的连接（环境）
        val conf = new SparkConf().setMaster("local").setAppName("WordCount")
        val sc = new SparkContext(conf)

        // 读取文件
        val lines = sc.textFile("data/word.txt")

        // 将文件中的数据进行了分词
        val words = lines.flatMap(_.split(" "))

        // 将分词后的数据进行了分组
        val wordGroup = words.groupBy(word => word)

        // 对分组后的数据进行统计分析
        val wordCount = wordGroup.mapValues(_.size)

        // 将统计结果打印在控制台上
        wordCount.collect().foreach(println)


        sc.stop()

    }
}
