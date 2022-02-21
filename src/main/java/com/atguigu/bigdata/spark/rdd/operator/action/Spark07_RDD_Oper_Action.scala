package com.atguigu.bigdata.spark.rdd.operator.action

import org.apache.spark.{SparkConf, SparkContext}

object Spark07_RDD_Oper_Action {

    def main(args: Array[String]): Unit = {

        // 一个应用程序, Driver程序
        val conf = new SparkConf().setMaster("local[*]").setAppName("RDD")
        val sc = new SparkContext(conf)

        // TODO 算子 - 行动
        val rdd = sc.makeRDD(
            List[Int](),2
        )

        val user = new User() // Driver
        rdd.foreach(
            num => {
                println(user.age + num) // Executor
            }
        )

        // Scala语法 ： 闭包
        // Spark在执行算子时，如果算子的内部使用了外部的变量（对象），那么意味着一定会出现闭包
        // 在这种场景中，需要将Driver端的变量通过网络传递给Executor端执行，这个操作不用执行也能判断出来
        // 可以在真正执行之前，对数据进行序列化校验，

        // Spark在执行作业前，需要先进行闭包检测功能。

        sc.stop()

    }
    class User {
        val age = 30
    }
}
