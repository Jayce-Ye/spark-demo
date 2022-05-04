package com.atguigu.bigdata.spark.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark07_RDD_Oper_Transform_1 {

    def main(args: Array[String]): Unit = {

        val conf = new SparkConf().setMaster("local[*]").setAppName("RDD")
        conf.set("spark.local.dir", "e:/test")
        val sc = new SparkContext(conf)

        // TODO 算子 - 转换
        val lines = sc.textFile("data/apache.log")

        val filterLines = lines.filter(
            line => {
                //line.contains("17/05/2015")
                val datas = line.split(" ")
                val time = datas(3)
                time.startsWith("17/05/2015")
            }
        )

        val r = filterLines.map(
            line => {
                val datas = line.split(" ")
                datas(6)
            }
        )

        r.collect().foreach(println)



        sc.stop()

    }
}
