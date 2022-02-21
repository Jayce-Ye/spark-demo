package com.atguigu.bigdata.spark.rdd.dep

import org.apache.spark.{SparkConf, SparkContext}

object Spark02_WordCount_Dep {

    def main(args: Array[String]): Unit = {

        val conf = new SparkConf().setMaster("local[*]").setAppName("WordCount")
        val sc = new SparkContext(conf)

        val lines = sc.textFile("data/word.txt")
        println(lines.dependencies) // OneToOneDependency
        println("*******************************")

        val words = lines.flatMap(_.split(" "))
        println(words.dependencies) // OneToOneDependency
        println("*******************************")

        val wordToOne = words.map((_,1)) // OneToOneDependency
        println(wordToOne.dependencies)
        println("*******************************")

        val wordToCount = wordToOne.reduceByKey(_+_) //
        println(wordToCount.dependencies)
        println("*******************************")

        wordToCount.collect().foreach(println)
        wordToCount.foreach(println)

        // 依赖的关系主要分为两大类：窄依赖（OneToOneDependency） & 宽依赖（ShuffleDependency）

        sc.stop()

    }
}
