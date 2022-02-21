package com.atguigu.bigdata.spark.rdd.instance

import org.apache.spark.{SparkConf, SparkContext}

object Spark02_RDD_Instance_File_Partition_Data {

    def main(args: Array[String]): Unit = {

        val conf = new SparkConf().setMaster("local[*]").setAppName("RDD")
        val sc = new SparkContext(conf)

        // TODO 从文件中创建RDD

        // TODO 1. 分区数据的处理也是由Hadoop决定的。
        // TODO 2. hadoop在计算分区时会处理数据时的逻辑不一样。
        // TODO 3. Spark读取文件数据底层使用的就是hadoop读取的，所以读取规则用的是hadoop
        //         3.1 hadoop读取数据是按行读取的，不是按字节读取
        //         3.2 hadoop读取数据是偏移量读取的
        //         3.3 hadoop读取数据时，不会重复读取相同的偏移量

        /*

          1@@ => 012
          2@@ => 345
          3   => 6

          计算读取偏移量
          [0, 3] => [12]
          [3, 6] => [3]
          [6, 7] => []

         */

        /*
         1. 计算有多少个分区？
            13 / 3 = 4
            13 / 4 = 3...1 = 3 + 1 = 4
         2. 计算每个分区放什么数据？

            123@@ => 01234
            456@@ => 56789
            789   => 101112
            ************************************

            [0, 4] => [123]
            [4, 8] => [456]
            [8, 12] => [789]
            [12, 13] => []


         */

        val rdd = sc.textFile("data/word.txt", 3)

        rdd.saveAsTextFile("output")


        sc.stop()

    }
}
