package com.atguigu.bigdata.spark.rdd.operator.transform

import org.apache.spark.{SparkConf, SparkContext}

object Spark17_RDD_Oper_Transform {

    def main(args: Array[String]): Unit = {

        val conf = new SparkConf().setMaster("local[*]").setAppName("RDD")
        conf.set("spark.local.dir", "e:/test")
        val sc = new SparkContext(conf)

        // TODO 将数据List(("a", 88), ("b", 95), ("a", 91), ("b", 93), ("a", 95), ("b", 98))求每个key的平均值
        val rdd = sc.makeRDD(
            List(
                ("a", 1), ("a", 2), ("b", 3),
                ("b", 4), ("b", 5), ("a", 6)
            ),
            2
        )
        val rdd1 = sc.makeRDD(
            List(
                ("a", (1,1)), ("a", 2), ("b", (3,1)),
                ("b", (4,1)), ("b", 5), ("a", (6,1))
            ),
            2
        )
        // groupByKey() => total / cnt = avg
        // reduceByKey() => total / cnt = avg
        // aggregateByKey(z)(f1, f2) => total / cnt = avg
        // foldByKey(z)(f1) => total / cnt = avg
        // (a, 3)(b, 4)

        // combineByKey算子有三个参数
        // 第一个参数表示 当第一个数据不符合我们的规则时，用于进行转换的操作
        // 第二个参数表示 分区内计算规则
        // 第三个参数表示 分区间计算规则
        val rdd2 = rdd.combineByKey(
            num => (num, 1),
            (x : (Int, Int), y) => {
                (x._1 + y, x._2 + 1)
            },
            ( x : (Int, Int), y:(Int, Int) ) => {
                (x._1 + y._1, x._2 + y._2)
            }
        )
        rdd2.collect.foreach(println)


        sc.stop()

    }
}
