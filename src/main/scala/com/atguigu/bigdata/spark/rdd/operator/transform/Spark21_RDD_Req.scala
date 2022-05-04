package com.atguigu.bigdata.spark.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark21_RDD_Req {

    def main(args: Array[String]): Unit = {

        val conf = new SparkConf().setMaster("local[*]").setAppName("Req")
        val sc = new SparkContext(conf)

        // TODO 统计出每一个省份每个广告被点击数量排行的Top3

        // TODO 1. 读取数据文件，获取原始数据
        //      1516609143867 6 7 64 16
        val lines = sc.textFile("data/agent.log")

        // TODO 2. 将原始数据进行结构的转换
        //     line => (（省份，广告），1)
        val wordToOne = lines.map(
            line => {
                val datas = line.split(" ")
                ( (datas(1), datas(4)), 1 )
            }
        )

        // TODO 3. 将转换结构后的进行统计
        //     (（省份，广告），1) => (（省份，广告），sum)
        val wordToSum = wordToOne.reduceByKey(_+_)

        // TODO 4. 将统计结果进行结构的转换，将省份独立出来
        //     (（省份，广告），sum) => (省份，(广告，sum))
        val wordToTuple = wordToSum.map {
            case ( (prv, adv), sum ) => {
                ( prv, (adv, sum) )
            }
        }

        // TODO 5. 将数据按照省份进行分组
        //    (省份，List[(广告，sum), (广告1，sum1), (广告2，sum2)])
        val groupRDD: RDD[(String, Iterable[(String, Int)])] = wordToTuple.groupByKey()

        // TODO 6. 将分组后的数据，根据点击数量进行排行（降序）
        // TODO 7. 将排序后的数据取前3
        val top3 = groupRDD.mapValues(
            iter => {
                iter.toList.sortBy(_._2)(Ordering.Int.reverse).take(3)
            }
        )

        // TODO 8. 将结果采集后打印在控制台上
        top3.collect.foreach(println)

        sc.stop()

    }
}
