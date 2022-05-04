package com.atguigu.bigdata.spark.req

import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

object Spark01_Req_HotCategoryTop10_1 {

    def main(args: Array[String]): Unit = {

        val conf = new SparkConf().setMaster("local[*]").setAppName("HotCategoryTop10")
        val sc = new SparkContext(conf)

        // TODO 需求一 ： Top10热门品类
        // TODO (找茬) :
        // 1. 同一个RDD的重复使用
        // 2. cogroup算子可能性能底下

        // TODO 读取文件，获取原始数据
        val fileDatas = sc.textFile("data/user_visit_action.txt")
        //fileDatas.persist(StorageLevel.MEMORY_AND_DISK)

        // TODO 统计品类的点击数量
        // 统计分区前需要将不需要的数据过滤掉
        // 先保留所有的点击数据
        val clickDatas = fileDatas.filter(
            data => {
                val datas = data.split("_")
                val cid = datas(6)
                cid != "-1"
            }
        )
        // 对点击数据进行统计
        val clickCntDatas = clickDatas.map(
            data => {
                val datas = data.split("_")
                val cid = datas(6)
                (cid, 1)
            }
        ).reduceByKey(_+_)

        // TODO 统计品类的下单数量
        // 先保留所有的下单数据
        val orderDatas = fileDatas.filter(
            data => {
                val datas = data.split("_")
                val cid = datas(8)
                cid != "null"
            }
        )
        // 对下单数据进行统计
        // (1,2,3,4) => ((1,2,3,4), 1)
        // 1, 2, 3, 4
        // (1,1),(2,1),(3,1),(4,1)
        val orderCntDatas = orderDatas.flatMap(
            data => {
                val datas = data.split("_")
                val cid = datas(8)
                val cids = cid.split(",")
                cids.map((_, 1))
            }
        ).reduceByKey(_+_)
        // TODO 统计品类的支付数量
        // 先保留所有的支付数据
        val payDatas = fileDatas.filter(
            data => {
                val datas = data.split("_")
                val cid = datas(10)
                cid != "null"
            }
        )
        // 对下单数据进行统计
        val payCntDatas = payDatas.flatMap(
            data => {
                val datas = data.split("_")
                val cid = datas(10)
                val cids = cid.split(",")
                cids.map((_, 1))
            }
        ).reduceByKey(_+_)

        // TODO 对统计结果进行排序 => Tuple(点击，下单，支付)
        // (品类ID, 点击) => ( 品类ID, ( 点击，0，0 ) )
        // (品类ID, 下单) => ( 品类ID, ( 0，下单，0 ) )
        // (品类ID, 支付) => ( 品类ID, ( 0，0，支付 ))
        // (品类ID, ( 点击，下单，支付 ))
        // 3 => 1 => （聚合）
        // reduceByKey
        // (( 点击，下单，支付 ), ( 点击，下单，支付 )) => ( 点击，下单，支付 )
        val clickMapDatas = clickCntDatas.map {
            case ( cid, cnt ) => {
                ( cid, (cnt, 0, 0) )
            }
        }
        val orderMapDatas = orderCntDatas.map {
            case ( cid, cnt ) => {
                ( cid, (0, cnt, 0) )
            }
        }
        val payMapDatas = payCntDatas.map {
            case ( cid, cnt ) => {
                ( cid, (0, 0, cnt) )
            }
        }

        val unionRDD: RDD[(String, (Int, Int, Int))] = clickMapDatas.union(orderMapDatas).union(payMapDatas)

        val reduceRDD = unionRDD.reduceByKey(
            (t1, t2) => {
                ( t1._1 + t2._1, t1._2 + t2._2, t1._3 + t2._3 )
            }
        )

        val top10 = reduceRDD.sortBy(_._2, false).take(10)


        // TODO 将结果采集后打印再控制台上
        top10.foreach(println)

        sc.stop()

    }
}
