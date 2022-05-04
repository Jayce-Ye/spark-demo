package com.atguigu.streaming

import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.streaming.receiver.Receiver

import java.io.{BufferedReader, InputStreamReader}
import java.net.Socket
import java.nio.charset.StandardCharsets

object SparkStreaming03_CustomReceiver {
  def main(args: Array[String]): Unit = {
    //1. 生成一个Dstream
    val sparkConf: SparkConf = new SparkConf()
      .setMaster("local[2]")
      .setAppName("StreamingTest")
    val streamingContext = new StreamingContext(sparkConf, Seconds(4))

    val myStream: ReceiverInputDStream[String] = streamingContext.receiverStream(new MyReceiver)
    myStream
      .flatMap(_.split(" "))
      .map((_,1))
      .reduceByKey(_+_)
      .print()

    //3. 运行流程序
    streamingContext.start()
    streamingContext.awaitTermination()
  }
}

class MyReceiver extends Receiver[String](StorageLevel.MEMORY_ONLY) {
  override def onStart(): Unit = {
    new Thread {
      override def run(): Unit = {
        val socket = new Socket("hadoop102", 9999)
        val reader = new BufferedReader(new InputStreamReader(socket.getInputStream, StandardCharsets.UTF_8))
        var line: String = reader.readLine()
        while (line != null) {
          store(line)
          line = reader.readLine()
        }
      }
    }.start()
  }

  override def onStop(): Unit = {

  }
}