package com.atguigu.streaming

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.{InputDStream, ReceiverInputDStream}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}

import java.lang

object SparkStreaming05_Transform {
  def main(args: Array[String]): Unit = {
    //1. 生成一个Dstream
    val sparkConf: SparkConf = new SparkConf()
      .setMaster("local[2]")
      .setAppName("StreamingTest")
    val streamingContext = new StreamingContext(sparkConf, Seconds(4))

    val kafkaDStream: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream(
      streamingContext,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](
        List("topic1"),
        Map(
          "key.deserializer" -> classOf[StringDeserializer],
          "value.deserializer" -> classOf[StringDeserializer],
          "bootstrap.servers" -> "hadoop105:9092,hadoop106:9092,hadoop107:9092",
          "group.id" -> "Stream01",
          "auto.offset.reset" -> "earliest",
          "enable.auto.commit" -> (false: lang.Boolean)
        ))
    )
    kafkaDStream
      .transform{
        rdd =>
          rdd.flatMap(_.value().split(" "))
            .map((_,1))
            .reduceByKey(_+_)
      }.print()

    //3. 运行流程序
    streamingContext.start()
    streamingContext.awaitTermination()
  }

}
