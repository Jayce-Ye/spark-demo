package com.atguigu.bigdata.spark.rdd.instance

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark02_RDD_Instance_File_Partition {

    def main(args: Array[String]): Unit = {

        val conf = new SparkConf().setMaster("local[*]").setAppName("RDD")
        val sc = new SparkContext(conf)

        // TODO 从文件中创建RDD

        // textFile方法可以在读取文件时，设定分区
        // 设定分区时，应该传递第二个参数，如果不设定，存在默认值
        //    math.min(defaultParallelism, 2)
        // 第二个参数表示最小分区数，所以最终的分区数量可以大于这个值的。

        // TODO 1. spark读取文件其实底层就是hadoop读取文件
        // TODO 2. spark的分区数量其实就来自于hadoop读取文件的切片

        // （想要切片数量）numSplits = 2
        // totalSize = 7
        // (预计每个分区的字节大小)goalSize = totalSize / numSplits = 7 / 2 = 3
        // splitSize = Math.max(minSize(1), Math.min(goalSize(5G), blockSize(128M))) = 128;
        // 7 / 3 = 2...1 = 2 + 1 = 3
        val rdd = sc.textFile("data/word.txt", 2)

        rdd.saveAsTextFile("output")


        sc.stop()

    }
}
