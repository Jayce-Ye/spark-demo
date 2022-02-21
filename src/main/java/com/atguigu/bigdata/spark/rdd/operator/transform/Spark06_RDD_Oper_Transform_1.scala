package com.atguigu.bigdata.spark.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark06_RDD_Oper_Transform_1 {

    def main(args: Array[String]): Unit = {

        val conf = new SparkConf().setMaster("local[*]").setAppName("RDD")
        conf.set("spark.local.dir", "e:/test")
        val sc = new SparkContext(conf)

        // TODO 算子 - 转换
//        val rdd = sc.makeRDD(
//            List("Hello", "hive", "hbase", "Hadoop")
//        )
//
//        val rdd1 = rdd.groupBy(_.substring(0, 1).toUpperCase())
//        rdd1.collect.foreach(println)

        // 从服务器日志数据apache.log中获取每个时间段访问量
        // (10, 100), (11, 101)
        val lines = sc.textFile("data/apache.log")

        // (time, List((time,1),(time, 1)))
        // TODO groupBy算子可以实现 WordCount ( 1 / 10 )
        val groupRDD: RDD[(String, Iterable[(String, Int)])] = lines.map(
            lines => {
                val datas = lines.split(" ")
                val time = datas(3)
                val times = time.split(":")
                (times(1), 1)
            }
        ).groupBy(_._1)

        val timeCnt: RDD[(String, Int)] = groupRDD.mapValues(_.size)
        timeCnt.collect.foreach(println)

        sc.stop()

    }
}
