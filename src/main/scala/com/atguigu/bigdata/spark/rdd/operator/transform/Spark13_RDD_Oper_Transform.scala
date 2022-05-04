package com.atguigu.bigdata.spark.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

object Spark13_RDD_Oper_Transform {

    def main(args: Array[String]): Unit = {

        val conf = new SparkConf().setMaster("local[*]").setAppName("RDD")
        conf.set("spark.local.dir", "e:/test")
        val sc = new SparkContext(conf)

        // TODO 算子 - 转换 - K-V
        val rdd : RDD[Int] = sc.makeRDD(
            List(1,2,3,4),2
        )
        rdd.sortBy(num=>num)
        //rdd.partitionBy(null);

        // partitionBy算子是根据指定的规则对每一条数据进行重分区
        // repartition : 强调分区数量的变化，数据怎么变不关心
        // partitionBy : 关心数据的分区规则

        val rdd1: RDD[(Int, Int)] = rdd.map((_, 1))

        // 下面调用RDD对象的partitionBy方法一定会报错。
        // 二次编译（隐式转换）
        // RDD => PairRDDFunctions
        // HashPartitioner是Spark中默认shuffle分区器
        rdd1.partitionBy(new HashPartitioner(2)).saveAsTextFile("output");





        sc.stop()

    }
}
