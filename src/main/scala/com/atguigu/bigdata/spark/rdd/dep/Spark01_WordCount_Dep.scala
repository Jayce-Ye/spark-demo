package com.atguigu.bigdata.spark.rdd.dep

import org.apache.spark.{SparkConf, SparkContext}

object Spark01_WordCount_Dep {

    def main(args: Array[String]): Unit = {

        val conf = new SparkConf().setMaster("local[*]").setAppName("WordCount")
        val sc = new SparkContext(conf)

        val lines = sc.textFile("data/word.txt")
        println(lines.toDebugString)
        println("*******************************")

        val words = lines.flatMap(_.split(" "))
        println(words.toDebugString)
        println("*******************************")

        val wordToOne = words.map((_,1))
        println(wordToOne.toDebugString)
        println("*******************************")

        val wordToCount = wordToOne.reduceByKey(_+_)
        println(wordToCount.toDebugString)
        println("*******************************")

        wordToCount.collect().foreach(println)

        sc.stop()

    }
}
