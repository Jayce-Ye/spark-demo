package com.atguigu.bigdata.spark.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark16_RDD_Oper_Transform {

    def main(args: Array[String]): Unit = {

        val conf = new SparkConf().setMaster("local[*]").setAppName("RDD")
        conf.set("spark.local.dir", "e:/test")
        val sc = new SparkContext(conf)

        // TODO 取出每个分区内相同key的最大值然后分区间相加
        // 【（a,1）,(a,2),(b,3)】
        //      => 【（a, 2）, (b, 3)】
        //                => 【 (a, 8), (b, 8) 】
        //      => 【(b, 5), (a, 6)】
        // 【（b,4）,(b,5),(a,6)】
        val rdd = sc.makeRDD(
            List(
                ("a",1),("a",2),("b",3),
                ("b",4),("b",5),("a",6)
            ),
            2
        )

        // aggregateByKey算子存在函数柯里化
        // 第一个参数列表中有一个参数
        //     参数为零值，表示计算初始值 zero, z, 用于数据进行分区内计算
        // 第二个参数列表中有两个参数
        //     第一个参数表示 分区内计算规则
        //     第二个参数表示 分区间计算规则
        val rdd1 = rdd.aggregateByKey(5)(
            (x, y) => {
                math.max(x, y)
            },
            (x, y) => {
                x + y
            }
        )

        // TODO aggregateByKey也可以实现WordCount ( 4 / 10 )
        val rdd2 = rdd.aggregateByKey(0)(_+_, _+_)

        // TODO foldByKey也可以实现WordCount ( 5 / 10 )
        // TODO 如果aggregateByKey算子的分区内计算逻辑和分区间计算逻辑相同，那么可以使用foldByKey算子简化
        val rdd3 = rdd.foldByKey(0)(_+_)

        rdd3.collect.foreach(println)

        sc.stop()

    }
}
