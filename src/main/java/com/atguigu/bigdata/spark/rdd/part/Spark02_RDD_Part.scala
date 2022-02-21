package com.atguigu.bigdata.spark.rdd.part

import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, Partitioner, SparkConf, SparkContext}

object Spark02_RDD_Part {

    def main(args: Array[String]): Unit = {


        val conf = new SparkConf().setMaster("local[*]").setAppName("WordCount")
        val sc = new SparkContext(conf)

        val lines = sc.makeRDD(
            List(
                ("nba", 1),
                ("cba", 1),
                ("nba", 1),
                ("wnba", 1)
            ),2
        )

//        val rdd1: RDD[(String, Int)] = lines.reduceByKey(_ + _)
//        val rdd2: RDD[(String, Int)] = rdd1.reduceByKey(_ + _)
         val rdd1: RDD[(String, Int)] = lines.partitionBy(new MyPartitioner())
         val rdd2: RDD[(String, Int)] = rdd1.partitionBy(new MyPartitioner())


        sc.stop()

    }
    // 自定义分区器
    // 1. 继承Partitioner
    // 2. 重写方法(2 + 2)
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

        override def equals(other: Any): Boolean = other match {
            case h: MyPartitioner =>
                h.numPartitions == numPartitions
            case _ =>
                false
        }

        override def hashCode: Int = numPartitions
    }
}
