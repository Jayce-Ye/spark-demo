package com.atguigu.bigdata.spark.summer.controller

import com.atguigu.bigdata.spark.summer.common.TController
import com.atguigu.bigdata.spark.summer.service.HotCategoryTop10Service

class HotCategoryTop10Controller extends TController {

    private val hotCategoryTop10Service = new HotCategoryTop10Service

    override def dispatch(): Unit = {
        val result: Array[(String, (Int, Int, Int))] = hotCategoryTop10Service.analysis()
        result.foreach(println)
    }
}
