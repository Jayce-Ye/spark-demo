package com.atguigu.bigdata.spark.rdd.persist

import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

object Spark02_RDD_Persist {

    def main(args: Array[String]): Unit = {


        val conf = new SparkConf().setMaster("local[*]").setAppName("WordCount")
        val sc = new SparkContext(conf)
        sc.setCheckpointDir("cp")

        val lines = sc.makeRDD(
            List("Hadoop Hive Hbase", "Spark scala Java")
        )
        val words = lines.flatMap(_.split(" "))
        val wordToOne = words.map(
            t => {
                println("*************************")
                (t, 1)
            }
        )

        // Spark可以将中间的计算结果保存到检查点中，让其他的应用使用数据
        // Checkpoint directory has not been set in the SparkContext
        wordToOne.checkpoint()

        val wordToCount = wordToOne.reduceByKey(_+_)
        println(wordToCount.toDebugString)
        wordToCount.collect()//.foreach(println)
        println("--------------------------------------------")
       // val rdd2: RDD[(Int, Iterable[(String, Int)])] = wordToOne.groupBy(_._2)
       // rdd2.collect()
        println(wordToCount.toDebugString)

        sc.stop()

    }
}
