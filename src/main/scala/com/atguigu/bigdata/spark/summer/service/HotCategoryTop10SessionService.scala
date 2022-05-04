package com.atguigu.bigdata.spark.summer.service

import com.atguigu.bigdata.spark.summer.bean.UserVisitAction
import com.atguigu.bigdata.spark.summer.common.TService
import com.atguigu.bigdata.spark.summer.dao.{HotCategoryTop10Dao, HotCategoryTop10SessionDao}
import org.apache.spark.rdd.RDD

class HotCategoryTop10SessionService extends TService {

    private val hotCategoryTop10SessionDao = new HotCategoryTop10SessionDao

    override def analysis( data : Any ) = {
        val topIds: Array[String] = data.asInstanceOf[Array[String]]

        val fileDatas = hotCategoryTop10SessionDao.readFileBySpark("data/user_visit_action.txt")

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

        val clickDatas = actionDatas.filter {
            data => {
                if ( data.click_category_id != -1 ) {
                    topIds.contains(data.click_category_id.toString)
                } else {
                    false
                }
            }
        }

        val reduceDatas = clickDatas.map(
            data => {
                (( data.click_category_id, data.session_id ), 1)
            }
        ).reduceByKey(_+_)

        val groupDatas: RDD[(Long, Iterable[(String, Int)])] = reduceDatas.map {
            case ((cid, sid), cnt) => {
                (cid, (sid, cnt))
            }
        }.groupByKey()

        groupDatas.mapValues(
            iter => {
                iter.toList.sortBy(_._2)(Ordering.Int.reverse).take(10)
            }
        ).collect()
    }
}
