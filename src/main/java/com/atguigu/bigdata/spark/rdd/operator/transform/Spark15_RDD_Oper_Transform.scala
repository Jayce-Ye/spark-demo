package com.atguigu.bigdata.spark.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark15_RDD_Oper_Transform {

    def main(args: Array[String]): Unit = {

        val conf = new SparkConf().setMaster("local[*]").setAppName("RDD")
        conf.set("spark.local.dir", "e:/test")
        val sc = new SparkContext(conf)

        // TODO 算子 - 转换 - groupByKey
        val rdd : RDD[(String, Int)] = sc.makeRDD(
            List(
                ("a", 1),
                ("a", 1),
                ("a", 1)
            )
        )
        val value: RDD[(String, Iterable[(String, Int)])] = rdd.groupBy(_._1)

        // TODO groupByKey & groupBy
        // 1. groupBy不需要考虑数据类型，groupByKey必须保证数据kv类型
        // 2. groupBy按照指定的规则进行分组，groupByKey必须根据key对value分组
        // 3. 返回结果类型
        //    groupByKey => (String, Iterable[Int])
        //    groupBy    => (String, Iterable[(String, Int)])

        // groupByKey算子将相同key数据的value分在一个组中
        // TODO groupByKey也可以实现 WordCount ( 3 / 10 )
        val rdd1: RDD[(String, Iterable[Int])] = rdd.groupByKey()

        val rdd2: RDD[(String, Int)] = rdd1.mapValues(_.size)

        rdd2.collect.foreach(println)


        sc.stop()

    }
}
