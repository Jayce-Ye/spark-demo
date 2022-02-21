package com.atguigu.bigdata.spark.summer.application

import com.atguigu.bigdata.spark.summer.common.TApplication
import com.atguigu.bigdata.spark.summer.controller.WordCountController


object WordCountApplication extends TApplication with App{

    execute(appName = "WordCount"){
        val controller = new WordCountController
        controller.dispatch()
    }
}
