package com.atguigu.bigdata.spark.rdd.serial

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark01_RDD_Serial {

    def main(args: Array[String]): Unit = {

        // 一个应用程序, Driver程序
        val conf = new SparkConf().setMaster("local[*]").setAppName("RDD")
        val sc = new SparkContext(conf)

        // TODO 算子 - 行动
        val rdd = sc.makeRDD(List(
            "Hello", "Hive", "Spark", "Scala"
        ))

        val s = new Search("S")
        s.filterByQuery(rdd).foreach(println)

        //new Test("zhangsan")


        sc.stop()

    }
    class Search( q : String )  {
        def filterByQuery( rdd : RDD[String] ): RDD[String] = {
            // 算子外 -> Driver
            // 算子内 -> Executor
            val s : String = this.q;
            rdd.filter(_.startsWith(s) )
        }
    }
    class Test( name:String ) {
        def test(): Unit = {
            println(name)
        }
    }
}
