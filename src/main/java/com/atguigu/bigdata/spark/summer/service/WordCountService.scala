package com.atguigu.bigdata.spark.summer.service

import com.atguigu.bigdata.spark.summer.common.TService
import com.atguigu.bigdata.spark.summer.dao.WordCountDao


class WordCountService extends TService {

    private val wordCountDao = new WordCountDao

    override def analysis() = {

        val lines = wordCountDao.readFile("data/word.txt")

        val words = lines.flatMap(
            line => {
                line.split(" ")
            }
        )

        val wordGroup = words.groupBy(word => word)
        val wordCount = wordGroup.mapValues(
            v => {
                v.size
            }
        )
        wordCount
    }
}
