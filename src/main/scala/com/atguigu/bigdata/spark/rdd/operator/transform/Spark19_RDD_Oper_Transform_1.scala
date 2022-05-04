package com.atguigu.bigdata.spark.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark19_RDD_Oper_Transform_1 {

    def main(args: Array[String]): Unit = {

        val conf = new SparkConf().setMaster("local[*]").setAppName("RDD")
        conf.set("spark.local.dir", "e:/test")
        val sc = new SparkContext(conf)

        // TODO 将数据List(("a", 88), ("b", 95), ("a", 91), ("b", 93), ("a", 95), ("b", 98))求每个key的平均值
        val rdd = sc.makeRDD(
            List(
                (new User(), 2), (new User(), 1), (new User(), 3), (new User(), 4)
            )
        )

        //  ("a", 1)("a", 2)("b", 4) ("c", 3)
        // sortByKey算子就是按照key排序
        val rdd1: RDD[(User, Int)] = rdd.sortByKey(false)

        rdd1.collect.foreach(println)


        sc.stop()

    }
    class User extends Ordered[User]{
        override def compare(that: User): Int = {
            1
        }
    }
}
