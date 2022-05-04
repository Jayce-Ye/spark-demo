package com.atguigu.bigdata.spark.req

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark01_Req_HotCategoryTop10_2 {

    def main(args: Array[String]): Unit = {

        val conf = new SparkConf().setMaster("local[*]").setAppName("HotCategoryTop10")
        val sc = new SparkContext(conf)

        // TODO 需求一 ： Top10热门品类
        // TODO (找茬) :

        // TODO 读取文件，获取原始数据
        val fileDatas = sc.textFile("data/user_visit_action.txt")

        val flatDatas = fileDatas.flatMap(
            data => {
                var datas = data.split("_")
                if ( datas(6) != "-1" ) {
                    // 点击数据的场合
                    List((datas(6), (1, 0, 0)))
                } else if ( datas(8) != "null" ) {
                    // 下单数据的场合
                    val id = datas(8)
                    val ids = id.split(",")
                    ids.map(
                        id => {
                            (id, (0, 1, 0))
                        }
                    )
                } else if ( datas(10) != "null" ) {
                    // 支付数据的场合
                    val id = datas(10)
                    val ids = id.split(",")
                    ids.map(
                        id => {
                            (id, (0, 0, 1))
                        }
                    )
                } else {
                    Nil
                }
            }
        )

        val top10 = flatDatas.reduceByKey(
            (t1, t2) => {
                ( t1._1 + t2._1, t1._2 + t2._2, t1._3 + t2._3 )
            }
        ).sortBy(_._2, false).take(10)

        // TODO 将结果采集后打印再控制台上
        top10.foreach(println)

        sc.stop()

    }
}
