package com.atguigu.bigdata.spark.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark07_RDD_Oper_Transform {

    def main(args: Array[String]): Unit = {

        val conf = new SparkConf().setMaster("local[*]").setAppName("RDD")
        conf.set("spark.local.dir", "e:/test")
        val sc = new SparkContext(conf)

        // TODO 算子 - 转换
        val rdd : RDD[Int] = sc.makeRDD(List(1,2,3,4,5,6))

        // filter算子可以按照指定的规则对每一条数据进行筛选过滤
        // 数据处理结果为true，表示数据保留，如果为false，数据就丢弃
        val rdd1 = rdd.filter(
            num => num % 2 == 1
        )

        rdd1.collect.foreach(println)



        sc.stop()

    }
}
