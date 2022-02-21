package com.atguigu.bigdata.spark.summer.common

import com.atguigu.bigdata.spark.summer.util.EnvCache
import org.apache.spark.SparkContext

import scala.io.{BufferedSource, Source}

trait TDao {
    def readFile(  path : String ) = {
        // e:/data/word.txt
        val source: BufferedSource = Source.fromFile(EnvCache.get() + path)
        val lines = source.getLines().toList
        source.close()
        lines
    }
    def readFileBySpark(  path : String ) = {
        EnvCache.get().asInstanceOf[SparkContext].textFile(path)
    }
}
