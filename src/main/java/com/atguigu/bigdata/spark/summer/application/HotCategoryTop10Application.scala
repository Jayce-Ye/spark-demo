package com.atguigu.bigdata.spark.summer.application

import com.atguigu.bigdata.spark.summer.common.TApplication
import com.atguigu.bigdata.spark.summer.controller.{HotCategoryTop10Controller, WordCountController}


object HotCategoryTop10Application extends TApplication with App{

    execute(appName = "HotCategoryTop10"){
        val controller = new HotCategoryTop10Controller
        controller.dispatch()
    }
}
