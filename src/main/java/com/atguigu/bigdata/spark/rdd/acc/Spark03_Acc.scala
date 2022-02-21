package com.atguigu.bigdata.spark.rdd.acc

import org.apache.spark.util.AccumulatorV2
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

object Spark03_Acc {

    def main(args: Array[String]): Unit = {

        val conf = new SparkConf().setMaster("local[*]").setAppName("WordCount")
        val sc = new SparkContext(conf)

        val rdd = sc.makeRDD(
            List(
                "scala",
                "scala",
                "scala",
                "scala",
                "scala",
                "scala",
                "spark",
                "spark",
                "spark",
                "spark"
            )
        )

        // TODO 创建累加器
        val acc = new WordCountAccumulator()
        // TODO 向Spark进行注册
        sc.register(acc, "wordCount")

        rdd.foreach(
            word => {
                // TODO 将单词放入到累加器中
                acc.add(word)
            }
        )

        // TODO 获取累加器的累加结果
        println(acc.value)


        sc.stop()

    }
    // 自定义数据累加器
    // 1. 继承AccumulatorV2
    // 2. 定义泛型
    //    IN : String
    //    OUT : Map[K, V]
    // 3. 重写方法（3 + 3）
    class WordCountAccumulator extends AccumulatorV2[String, mutable.Map[String, Int]] {

        private val wcMap = mutable.Map[String,Int]()

        // 判断累加器是否为初始状态
        // copyAndReset must return a zero value copy
        // TODO 3. true
        override def isZero: Boolean = {
            wcMap.isEmpty
        }

        // TODO 1.
        override def copy(): AccumulatorV2[String, mutable.Map[String, Int]] = {
            new WordCountAccumulator()
        }

        // 重置累加器
        // TODO 2.
        override def reset(): Unit = {
            wcMap.clear()
        }

        // 从外部向累加器中添加数据
        override def add(word: String): Unit = {
            val oldCnt = wcMap.getOrElse(word, 0)
            wcMap.update(word, oldCnt + 1)
        }

        // 合并两个累加器的结果
        override def merge(other: AccumulatorV2[String, mutable.Map[String, Int]]): Unit = {

            other.value.foreach {
                case (word, cnt) => {
                    val oldCnt = wcMap.getOrElse(word, 0)
                    wcMap.update( word, oldCnt + cnt )
                }
            }
        }

        // 将结果返回到外部
        override def value: mutable.Map[String, Int] = wcMap
    }
}
