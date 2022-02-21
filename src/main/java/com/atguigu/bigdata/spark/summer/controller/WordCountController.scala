package com.atguigu.bigdata.spark.summer.controller

import com.atguigu.bigdata.spark.summer.common.TController
import com.atguigu.bigdata.spark.summer.service.WordCountService


class WordCountController extends TController {

    private val wordCountService = new WordCountService

    // 调度
    def dispatch(): Unit = {
        val wordCount: Map[String, Int] = wordCountService.analysis()
        println(wordCount)
    }
}
