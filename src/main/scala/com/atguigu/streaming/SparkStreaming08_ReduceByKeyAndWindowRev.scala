package com.atguigu.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

object SparkStreaming08_ReduceByKeyAndWindowRev {
  def main(args: Array[String]): Unit = {
    val context: StreamingContext = StreamingContext.getOrCreate(
      "./ck",
      () => {
        //1. 生成一个Dstream
        val sparkConf: SparkConf = new SparkConf()
          .setMaster("local[2]")
          .setAppName("StreamingTest")
        val streamingContext = new StreamingContext(sparkConf, Seconds(3))

        streamingContext.checkpoint("./ck")

        val dStream: ReceiverInputDStream[String] = streamingContext.socketTextStream("hadoop102", 9999)

        //2. 计算(wordcount)
        dStream
          .flatMap(_.split(" "))
          .map((_, 1))
          //      .window(
          //        Seconds(12),
          //        Seconds(6)
          //      )
          //      .reduceByKey(_+_)
          .reduceByKeyAndWindow(
            (x: Int, y: Int) => x + y,
            (x: Int, y: Int) => x - y,
            Seconds(12),
            Seconds(6)
          )
          .print()
        streamingContext
      })

    context.start()
    context.awaitTermination()
  }

}
