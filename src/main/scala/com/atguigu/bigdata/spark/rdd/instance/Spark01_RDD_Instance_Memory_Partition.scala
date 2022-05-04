package com.atguigu.bigdata.spark.rdd.instance

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark01_RDD_Instance_Memory_Partition {

    def main(args: Array[String]): Unit = {

        val conf = new SparkConf().setMaster("local[*]").setAppName("RDD")
        conf.set("spark.default.parallelism", "4")
        val sc = new SparkContext(conf)

        // TODO 从内存中创建RDD - 分区

        // 1. 如果构建RDD时，没有指定数据处理分区的数量，那么会使用默认分区数量
        // makeRDD方法存在第二个参数，这个参数表示分区数量numSlices（存在默认值）
        // scheduler.conf.getInt("spark.default.parallelism", totalCores)
        // totalCores : 当前Master环境的总（虚拟）核数
        // 分区设置的优先级 ： 方法参数 > 配置参数 > 环境配置

        // kafka生产者分区策略
        // 【1，3,5】【2，4】 : 轮询
        // 【1，2, 3】【5，4】 ：范围
        // 【1，2】【3，4】【5】 ：范围
        // Spark分区策略
        // 【1，2】【3，4，5】 ：范围
        // 【1】【2，3】【4，5】：范围
        val rdd1 : RDD[Int] = sc.makeRDD(
            Seq(1,2,3,4,5), 3
        )

        // saveAsTextFile方法可以生成分区文件
        rdd1.saveAsTextFile("output")


        sc.stop()

    }
}
