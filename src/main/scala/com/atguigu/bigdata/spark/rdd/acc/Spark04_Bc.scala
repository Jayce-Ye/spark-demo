package com.atguigu.bigdata.spark.rdd.acc

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.util.AccumulatorV2
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

object Spark04_Bc {

    def main(args: Array[String]): Unit = {

        val conf = new SparkConf().setMaster("local[*]").setAppName("WordCount")
        val sc = new SparkContext(conf)

        val rdd1 = sc.makeRDD(
            List(
                ("a", 1), ("b", 2)
            )
        )

//        val rdd2 = sc.makeRDD(
//            List(
//                ("a", 3), ("b", 4)
//            )
//        )
        val map = mutable.Map[String, Int](
            ("a", 3), ("b", 4)
        )
        val bcMap: Broadcast[mutable.Map[String, Int]] = sc.broadcast(map)


        val rdd3 = rdd1.map {
            case ( word, cnt ) => {

                val cnt2 = bcMap.value.getOrElse(word, 0)

                (word, (cnt, cnt2))
            }
        }

        rdd3.collect.foreach(println)

        sc.stop()

    }
}
