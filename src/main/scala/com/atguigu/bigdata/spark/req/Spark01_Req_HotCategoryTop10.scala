package com.atguigu.bigdata.spark.req

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark01_Req_HotCategoryTop10  {

    def main(args: Array[String]): Unit = {

        val conf = new SparkConf().setMaster("local[*]").setAppName("HotCategoryTop10")
        val sc = new SparkContext(conf)

        // TODO 需求一 ： Top10热门品类

        // TODO 读取文件，获取原始数据
        val fileDatas = sc.textFile("data/user_visit_action.txt")

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
//        val clickSortedDatas = clickCntDatas.sortBy(_._2, false)
//        clickSortedDatas.zipWithIndex()
        // (品类ID, 点击) Data
        // (品类ID, 下单) Data
        // (品类ID, 支付) Data
        // (品类ID, ( 点击，下单，支付 ))
        // join leftOuterJoin
        //clickCntDatas.fullOuterJoin(orderCntDatas).fullOuterJoin(payCntDatas)
        val cidCntsDatas: RDD[(String, (Iterable[Int], Iterable[Int], Iterable[Int]))] =
            clickCntDatas.cogroup(orderCntDatas, payCntDatas)

        val mapDatas = cidCntsDatas.map {
            case ( cid, (clickIter, orderIter, payIter) ) => {
                var clickcnt = 0
                var ordercnt = 0
                var paycnt = 0

                val iterator1: Iterator[Int] = clickIter.iterator
                if (iterator1.hasNext) {
                    clickcnt = iterator1.next()
                }

                val iterator2: Iterator[Int] = orderIter.iterator
                if (iterator2.hasNext) {
                    ordercnt = iterator2.next()
                }

                val iterator3: Iterator[Int] = payIter.iterator
                if (iterator3.hasNext) {
                    paycnt = iterator3.next()
                }

                (cid, (clickcnt, ordercnt, paycnt))
            }
        }

        val top10: Array[(String, (Int, Int, Int))] = mapDatas.sortBy(_._2, false).take(10)

        // TODO 将结果采集后打印再控制台上
        top10.foreach(println)

        sc.stop()

    }
}
