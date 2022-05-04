package com.atguigu.bigdata.spark.req

import com.atguigu.bigdata.spark.summer.bean.UserVisitAction
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark03_Req_PageFlow  {

    def main(args: Array[String]): Unit = {

        val conf = new SparkConf().setMaster("local[*]").setAppName("Pageflow")
        val sc = new SparkContext(conf)

        val fileDatas = sc.textFile("data/user_visit_action.txt")

        val actionDatas = fileDatas.map(
            data => {
                val datas = data.split("_")
                UserVisitAction(
                    datas(0),
                    datas(1).toLong,
                    datas(2),
                    datas(3).toLong,
                    datas(4),
                    datas(5),
                    datas(6).toLong,
                    datas(7).toLong,
                    datas(8),
                    datas(9),
                    datas(10),
                    datas(11),
                    datas(12).toLong
                )
            }
        )
        actionDatas.cache()

        // 【1，2，3，4，5，6，7】
        val okIds = List(1,2,3,4,5,6,7)
        // 【（1，2），（2，3）】
        val okFlowIds = okIds.zip(okIds.tail)

        // TODO 分母的计算
        val result: Map[Long, Int] = actionDatas.filter(
            action => {
                okIds.init.contains(action.page_id.toInt)
            }
        ).map(
            action => {
                (action.page_id, 1)
            }
        ).reduceByKey(_ + _).collect().toMap

        // TODO 分子的计算
        //  将数据按照session进行分组
        val groupRDD: RDD[(String, Iterable[UserVisitAction])] = actionDatas.groupBy(_.session_id)

        // 将分组后的数据进行组内排序
        val mapRDD = groupRDD.mapValues(
            iter => {
                val actions: List[UserVisitAction] = iter.toList.sortBy(_.action_time)
                //【1,2,3,4,5,6,7】
                //【2,3,4,5,6,7】
                // 滑窗
                //【1-2，2-3，3-4，4-5，5-6，6-7】
                val ids: List[Int] = actions.map(_.page_id.toInt)

                //                val iterator: Iterator[List[Long]] = ids.sliding(2)
                //                while ( iterator.hasNext ) {
                //                    val longs: List[Long] = iterator.next()
                //                    (longs.head, longs.last)
                //                }
                val flowIds: List[(Int, Int)] = ids.zip(ids.tail)

                flowIds.filter(
                    ids => {
                        okFlowIds.contains(ids)
                    }
                )
            }
        )
        val mapRDD2 = mapRDD.map(_._2)
        val flatRDD = mapRDD2.flatMap(list => list)

        // 分子计算完毕
        val reduceRDD = flatRDD.map((_, 1)).reduceByKey(_ + _)
        // TODO 单挑转换率的统计
        reduceRDD.foreach {
            case ( (id1, id2), cnt ) => {
                println(s"页面【${id1}-${id2}】单挑转换率为 ：" + ( cnt.toDouble / result.getOrElse(id1, 1) ))
            }
        }


        sc.stop()

    }
}
