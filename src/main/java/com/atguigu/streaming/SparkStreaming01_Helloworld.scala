package com.atguigu.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

object SparkStreaming01_Helloworld {
  def main(args: Array[String]): Unit = {
    //1. 生成一个Dstream
    val sparkConf: SparkConf = new SparkConf()
      .setMaster("local[2]")
      .setAppName("StreamingTest")
    val streamingContext = new StreamingContext(sparkConf, Seconds(4))

    val dStream: ReceiverInputDStream[String] = streamingContext.socketTextStream("hadoop102", 9999)

    //2. 计算(wordcount)
    dStream
      .flatMap(_.split(" "))
      .map((_,1))
      .reduceByKey(_+_)
      .print()

    //3. 运行流程序
    streamingContext.start()
    streamingContext.awaitTermination()
  }

}
