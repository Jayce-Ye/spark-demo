package com.atguigu.bigdata.spark.wc

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

object WordCount_1 {

    def main(args: Array[String]): Unit = {

        val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("WoedCount")

        val sc = new SparkContext(conf)

        val list: RDD[String] = sc.makeRDD(
            List("hello scala", "hello spark", "hadoop hive")
        )

        //处理数据，转换格式
        val wordRDD = list.flatMap(
            list => {
                val words: Array[String] = list.split(" ")
                words.map(
                    word => {
                        // hello => Map[(hello, 1)]
                        mutable.Map((word, 1))
                    }
                )
            }
        )
        // (T1(map), T2(map)) => T3(map)
        val map : mutable.Map[String, Int] = wordRDD.reduce(
            (map1, map2) => {

                map2.foreach {
                    case (word, cnt) => {
                        val oldCnt = map1.getOrElse(word, 0)
                        map1.update(word, oldCnt + cnt)
                    }
                }

                map1
            }
        )

        //wordRDD.aggregate()

        //println(map)

    }

}
