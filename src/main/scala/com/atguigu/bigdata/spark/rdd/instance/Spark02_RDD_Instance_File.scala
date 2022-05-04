package com.atguigu.bigdata.spark.rdd.instance

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark02_RDD_Instance_File {

    def main(args: Array[String]): Unit = {

        val conf = new SparkConf().setMaster("local[*]").setAppName("RDD")
        val sc = new SparkContext(conf)


        // TODO 从文件中创建RDD
        //  textFile方法可以通过路径获取数据，所以可以将文件作为数据处理的数据源
        //  textFile路径可以是相对路径，也可以是绝对路径，甚至可以为HDFS路径
        //  textFile路径不仅仅可以为文件路径，也可以为目录路径, 还可以为通配符路径
        //val rdd: RDD[String] = sc.textFile("data/word*.txt")

        // 如果读取文件后，想要获取文件数据的来源
        val rdd: RDD[(String, String)] = sc.wholeTextFiles("data/word*.txt")

        rdd.collect().foreach(println)



        sc.stop()

    }
}
