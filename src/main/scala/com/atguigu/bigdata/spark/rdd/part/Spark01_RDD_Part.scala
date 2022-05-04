package com.atguigu.bigdata.spark.rdd.part

import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{Partitioner, SparkConf, SparkContext}

object Spark01_RDD_Part {

    def main(args: Array[String]): Unit = {


        val conf = new SparkConf().setMaster("local[*]").setAppName("WordCount")
        val sc = new SparkContext(conf)

        val lines = sc.makeRDD(
            List(
                ("nba", "xxxxxx"),
                ("cba", "11111"),
                ("nba", "yyyyy"),
                ("wnba", "22222")
            ),2
        )

        val rdd2: RDD[(String, String)] = lines.partitionBy(new MyPartitioner())

        rdd2.saveAsTextFile("output")

        sc.stop()

    }
    // 自定义分区器
    // 1. 继承Partitioner
    // 2. 重写方法
    class MyPartitioner extends Partitioner {
        // TODO 分区数量
        override def numPartitions: Int = {
            3
        }

        // TODO 根据数据的key返回所在的分区编号，从0开始
        override def getPartition(key: Any): Int = {
            key match {
                case "nba" => 0
                case "cba" => 1
                case "wnba" => 2
            }
        }
    }
}
