package com.atguigu.streaming

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable

object SparkStreaming10_ReduceByWindow {
  def main(args: Array[String]): Unit = {
    //1. 生成一个DStream
    val sparkConf: SparkConf = new SparkConf()
      .setMaster("local[2]")
      .setAppName("StreamingTest")
    val streamingContext = new StreamingContext(sparkConf, Seconds(4))

    //2. 生成DStream
    val queue: mutable.Queue[RDD[Int]] = mutable.Queue.empty[RDD[Int]]
    val QueueDStream: InputDStream[Int] = streamingContext.queueStream(queue, oneAtATime = false)
    QueueDStream
      .reduceByWindow(
        (x: Int, y:Int) => x + y,
        Seconds(8),
        Seconds(4)
      )
      .print()


    //3. 运行流程序
    streamingContext.start()

    //4. 向队列中添加RDD
    while (true) {
      val rdd: RDD[Int] = streamingContext.sparkContext.makeRDD(Seq(1, 2, 3, 4, 5))
      queue.enqueue(rdd)
      Thread.sleep(2000)
    }
    streamingContext.awaitTermination()
  }

}
