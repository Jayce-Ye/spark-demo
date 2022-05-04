package com.atguigu.bigdata.spark.summer.util

object EnvCache {

    private val envCache : ThreadLocal[Object] = new ThreadLocal[Object]

    def put( data : Object ): Unit = {
        envCache.set(data)
    }

    def get() = {
        envCache.get()
    }

    def clear(): Unit = {
        envCache.remove()
    }
}
