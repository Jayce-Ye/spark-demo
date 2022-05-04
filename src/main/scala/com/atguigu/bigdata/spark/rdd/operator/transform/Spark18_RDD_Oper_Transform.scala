package com.atguigu.bigdata.spark.rdd.operator.transform

import org.apache.spark.{SparkConf, SparkContext}

object Spark18_RDD_Oper_Transform {

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

        // groupByKey() => total / cnt = avg
        // reduceByKey() => total / cnt = avg
        // aggregateByKey(z)(f1, f2) => total / cnt = avg
        // foldByKey(z)(f1) => total / cnt = avg
        // (a, 3)(b, 4)

        // TODO combineByKey算子也可以实现wordCount ( 6 / 10)

        val rdd2 = rdd.combineByKey(
            num => num,
            (x : Int, y) => {
                x + y
            },
            ( x : Int, y:Int ) => {
                x + y
            }
        )
        //rdd.combineByKey(num=>num)(_+_, _+_)
        rdd.groupByKey()
        rdd.reduceByKey(_+_)
        rdd.aggregateByKey(0)(_+_, _+_)
        rdd.foldByKey(0)(_+_)

        rdd2.collect.foreach(println)


        sc.stop()

    }
}
