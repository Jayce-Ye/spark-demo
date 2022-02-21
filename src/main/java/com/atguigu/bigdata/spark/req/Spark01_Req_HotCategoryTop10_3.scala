package com.atguigu.bigdata.spark.req

import org.apache.spark.util.AccumulatorV2
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

object Spark01_Req_HotCategoryTop10_3 {

    def main(args: Array[String]): Unit = {

        val conf = new SparkConf().setMaster("local[*]").setAppName("HotCategoryTop10")
        val sc = new SparkContext(conf)

        val fileDatas = sc.textFile("data/user_visit_action.txt")

        // 创建累加器对象
        val acc = new HotCategoryAccumulator()
        // 注册累加器
        sc.register(acc, "HotCategory")

        fileDatas.foreach(
            data => {
                val datas = data.split("_")
                if ( datas(6) != "-1" ) {
                    // 点击的场合
                    acc.add( (datas(6), "click") )
                } else if ( datas(8) != "null" ) {
                    // 下单的场合
                    val id = datas(8)
                    val ids = id.split(",")
                    ids.foreach(
                        id => {
                            acc.add( (id, "order") )
                        }
                    )
                } else if ( datas(10) != "null" ) {
                    // 支付的场合
                    val id = datas(10)
                    val ids = id.split(",")
                    ids.foreach(
                        id => {
                            acc.add( (id, "pay") )
                        }
                    )
                }
            }
        )

        // TODO 获取累加器的结果
        val resultMap: mutable.Map[String, HotCategoryCount] = acc.value
        val top10 = resultMap.map(_._2).toList.sortWith(
            (left, right) => {
                if ( left.clickCnt > right.clickCnt ) {
                    true
                } else if ( left.clickCnt == right.clickCnt ) {
                    if ( left.orderCnt > right.orderCnt ) {
                        true
                    } else if ( left.orderCnt == right.orderCnt ) {
                        left.payCnt > right.payCnt
                    } else {
                        false
                    }
                } else {
                    false
                }
            }
        ).take(10)

        top10.foreach(println)

        sc.stop()

    }
    case class HotCategoryCount( cid:String, var clickCnt : Int, var orderCnt : Int, var payCnt : Int )
    // TODO 自定义热门点击累加器
    //  1. 继承AccumulatorV2
    //  2. 定义泛型
    //     IN : (品类ID，行为类型)
    //     OUT : Map[品类ID, HotCategoryCount]
    //  3. 重写方法 （3 + 3）
    class HotCategoryAccumulator extends AccumulatorV2[(String, String), mutable.Map[String, HotCategoryCount]]{

        private val map = mutable.Map[String, HotCategoryCount]()

        override def isZero: Boolean = {
            map.isEmpty
        }

        override def copy(): AccumulatorV2[(String, String), mutable.Map[String, HotCategoryCount]] = {
            new HotCategoryAccumulator()
        }

        override def reset(): Unit = {
            map.clear()
        }

        override def add(v: (String, String)): Unit = {
            val (cid, actionType) = v
            val hcc: HotCategoryCount = map.getOrElse(cid, HotCategoryCount(cid, 0, 0, 0))
            actionType match {
                case "click" => hcc.clickCnt += 1
                case "order" => hcc.orderCnt += 1
                case "pay" => hcc.payCnt += 1
            }
            map.update(cid, hcc)
        }

        override def merge(other: AccumulatorV2[(String, String), mutable.Map[String, HotCategoryCount]]): Unit = {
            other.value.foreach {
                case ( cid, otherHCC ) => {
                    val thisHCC: HotCategoryCount = map.getOrElse(cid, HotCategoryCount(cid, 0, 0, 0))
                    thisHCC.clickCnt += otherHCC.clickCnt
                    thisHCC.orderCnt += otherHCC.orderCnt
                    thisHCC.payCnt += otherHCC.payCnt
                    map.update(cid, thisHCC)
                }
            }
        }

        override def value: mutable.Map[String, HotCategoryCount] = {
            map
        }
    }
}
