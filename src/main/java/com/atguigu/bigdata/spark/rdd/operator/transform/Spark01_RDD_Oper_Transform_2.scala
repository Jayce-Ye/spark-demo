package com.atguigu.bigdata.spark.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark01_RDD_Oper_Transform_2 {

    def main(args: Array[String]): Unit = {

        val conf = new SparkConf().setMaster("local[*]").setAppName("RDD")
        val sc = new SparkContext(conf)

        // TODO 算子 - 转换 - map
        // 从服务器日志数据apache.log中获取用户请求URL资源路径
        val lineRDD = sc.textFile("data/apache.log")

        // flatMap => 扁平化 => 整体（个体）
        // long string(A) => short string(B)

        val urlRDD = lineRDD.map(
            line => {
                val datas = line.split(" ")
                datas(6)
            }
        )

        urlRDD.collect().foreach(println)

        sc.stop()

    }
}
