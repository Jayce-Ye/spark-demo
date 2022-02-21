package com.atguigu.bigdata.spark.wc

import org.apache.spark.{SparkConf, SparkContext}

object Spark03_WordCount {

    def main(args: Array[String]): Unit = {

        // TODO 使用Spark
        // Spark是一个计算【框架】。
        // 1. 能找到他 ：增加依赖
        // 2. 获取Spark的连接（环境）
        val conf = new SparkConf().setMaster("local[*]").setAppName("WordCount")
        val sc = new SparkContext(conf)
        sc.setLogLevel("error");

        // 读取文件
        val lines = sc.textFile("data/word.txt")

        // 将文件中的数据进行了分词
        // word => (word, 1)
        val words = lines.flatMap(_.split(" "))
        val wordToOne = words.map((_,1))

        // reduceByKey : 按照key分组， 对相同的key的value进行reduce
        // (word, 1)(word, 1)(word, 1)(word, 1)(word, 1)
        // reduce(1,1,1,1,1)
        // 框架的核心就是封装
        val wordToCount = wordToOne.reduceByKey(_+_)

        // 将统计结果打印在控制台上
        wordToCount.collect().foreach(println)


        sc.stop()

    }
}
