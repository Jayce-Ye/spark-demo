package com.atguigu.bigdata.spark.wc

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

object WordCount {

    def main(args: Array[String]): Unit = {

        val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("WoedCount")

        val sc = new SparkContext(conf)

        val list: RDD[String] = sc.makeRDD(
            List("hello scala", "hello spark", "hadoop hive")
        )

        //处理数据，转换格式
        val wordRDD: RDD[(String, Int)] = list.flatMap(
            list => {
                val words: Array[String] = list.split(" ")
                words.map((_, 1))
            }
        )

        //aggregate
        val wordCount: mutable.Map[String, Int] = wordRDD.aggregate(mutable.Map[String, Int]())(
            //分区内的计算规则
            (map, kv) => {
                val word: String = kv._1
                val oldCnt: Int = kv._2
                val newCnt = map.getOrElse(word, 0) + oldCnt
                map.updated(word, newCnt)

            },
            //分区间的计算规则
            (map1, map2) => {
                map1.foldLeft(map2)(
                    (map, kv) => {
                        val word = kv._1
                        val oldCnt = kv._2
                        val newCnt = map.getOrElse(word, 0) + oldCnt
                        map.updated(word, newCnt)
                    }
                )
            }
        )

        println(wordCount)

    }

}
