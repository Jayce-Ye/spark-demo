package com.atguigu.bigdata.spark.summer.application

import com.atguigu.bigdata.spark.summer.common.TApplication
import com.atguigu.bigdata.spark.summer.controller.{HotCategoryTop10Controller, HotCategoryTop10SessionController}


object HotCategoryTop10SessionApplication extends TApplication with App{

    execute(appName = "HotCategoryTop10Session"){
        val controller = new HotCategoryTop10SessionController
        controller.dispatch()
    }
}
