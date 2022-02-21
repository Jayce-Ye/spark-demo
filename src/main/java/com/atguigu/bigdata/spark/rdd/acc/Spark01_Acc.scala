package com.atguigu.bigdata.spark.rdd.acc

import org.apache.spark.{SparkConf, SparkContext}

object Spark01_Acc  {

    def main(args: Array[String]): Unit = {

        val conf = new SparkConf().setMaster("local[*]").setAppName("WordCount")
        val sc = new SparkContext(conf)

        val rdd = sc.makeRDD(
            List(1,2,3,4),2
        )

        var sum = 0; // Driver
        rdd.foreach(
            num => {
                sum = sum + num; // Executor
            }
        )

        println(sum);

        sc.stop()

    }
}
