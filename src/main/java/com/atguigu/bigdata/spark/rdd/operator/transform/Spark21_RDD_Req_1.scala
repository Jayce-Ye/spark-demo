package com.atguigu.bigdata.spark.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark21_RDD_Req_1 {

    def main(args: Array[String]): Unit = {

        val conf = new SparkConf().setMaster("local[*]").setAppName("Req")
        val sc = new SparkContext(conf)

        // TODO 统计出每一个省份每个广告被点击数量排行的Top3

        // TODO 1. 读取数据文件，获取原始数据
        //      1516609143867 6 7 64 16
        val lines = sc.textFile("data/agent.log")

        // TODO 2. 将原始数据进行结构的转换
        //     line => (省份，（广告，1))
        val wordToOne = lines.map(
            line => {
                val datas = line.split(" ")
                (datas(1), (datas(4), 1 ))
            }
        )

        val groupRDD: RDD[(String, Iterable[(String, Int)])] = wordToOne.groupByKey()

        val top3 = groupRDD.mapValues(
            iter => {
                val wordCountMap: Map[String, Int] = iter.groupBy(_._1).mapValues(_.size)
                wordCountMap.toList.sortBy(_._2)(Ordering.Int.reverse).take(3)
            }
        )

        top3.collect.foreach(println)


        sc.stop()

    }
}
