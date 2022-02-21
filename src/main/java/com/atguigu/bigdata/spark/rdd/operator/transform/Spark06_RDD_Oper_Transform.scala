package com.atguigu.bigdata.spark.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark06_RDD_Oper_Transform {

    def main(args: Array[String]): Unit = {

        val conf = new SparkConf().setMaster("local[*]").setAppName("RDD")
        conf.set("spark.local.dir", "e:/test")
        val sc = new SparkContext(conf)

        // TODO 算子 - 转换
        val rdd : RDD[Int] = sc.makeRDD(List(1,2,3,4,5,6))

        // groupBy算子根据函数计算结果进行分组。
        // groupBy算子执行结果为KV数据类型
        //     k是为分组的标识， v就是同一个组的数据集合
        val rdd1: RDD[(Int, Iterable[Int])] = rdd.groupBy(_ % 2)
        //rdd1.mapValues()
        // 1 => 1
        // 2 => 0
        // 3 => 1
        // 4 => 0
        // 5 => 1
        // 6 => 0





        sc.stop()

    }
}
