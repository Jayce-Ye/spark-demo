package com.atguigu.app

import com.atguigu.util.{JDBCUtil, PropertiesUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Minutes, Seconds, StreamingContext}

import java.sql.Connection
import java.text.SimpleDateFormat
import java.util.{Date, Properties}

object RealTimeAPP {
  private val dateFormat = new SimpleDateFormat("yyyy-MM-dd")
  private val timeFormat = new SimpleDateFormat("HH:mm")

  def main(args: Array[String]): Unit = {
    //获取ConfigurationProperties
    val config: Properties = PropertiesUtil.load("config.properties")
    //1. 生成kafkaDStream
    val conf: SparkConf = new SparkConf()
      .setMaster("local[2]")
      .setAppName("RealTimeAPP")
    val ssc = new StreamingContext(conf, Seconds(3))
    val kafkaSource: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream(
      ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](
        List(config.getProperty("kafka.topic")),
        Map(
          "key.deserializer" -> classOf[StringDeserializer],
          "value.deserializer" -> classOf[StringDeserializer],
          "bootstrap.servers" -> config.getProperty("kafka.broker.list"),
          "group.id" -> "RT01",
          "auto.offset.reset" -> "earliest",
        ))
    )
    val odsLog: DStream[UserAction] = kafkaSource.map(_.value().split(" ")).map(
      fields =>
        UserAction(
          fields(0).toLong,
          fields(1),
          fields(2),
          fields(3).toInt,
          fields(4).toInt
        )
    )
    //2. 需求一: 过滤黑名单用户
    val filteredOdsLog: DStream[UserAction] = odsLog.filter(x => {
      val connection: Connection = JDBCUtil.getConnection
      val blackList: List[Int] = JDBCUtil.getBlackList(connection)
      connection.close()
      !blackList.contains(x.userId)
    })

    //3. 统计用户点击广告数量
    val userClicks: DStream[UserClick] = filteredOdsLog.map {
      case UserAction(timestamp, area, city, userId, adId) =>
        ((dateFormat.format(new Date(timestamp)), userId, adId), 1)
    }
      .reduceByKey(_ + _)
      .map {
        case ((dt, userId, adId), count) =>
          UserClick(dt, userId, adId, count)
      }

    //4. 用结果更新点击表格, 并根据点击表格内容更新黑名单
    userClicks.foreachRDD(
      rdd => {
        //更新点击数
        rdd.foreachPartition {
          iter =>
            val connection: Connection = JDBCUtil.getConnection
            JDBCUtil.updateUserClick(connection, iter.toList)
            connection.close()
        }
        //获取新的黑名单
        val connection: Connection = JDBCUtil.getConnection
        val blackList: List[Int] = JDBCUtil.getNewBlackList(connection)
        //更新黑名单列表
        JDBCUtil.updateBlackList(connection, blackList)
        connection.close()

      }
    )

    //5. 需求二: 统计每天各个地区各个城市各个广告的点击量
    val cityClick: DStream[CityClick] = filteredOdsLog.map {
      case UserAction(timestamp, area, city, userId, adId) =>
        ((dateFormat.format(new Date(timestamp)), area, city, adId), 1)
    }
      .reduceByKey(_ + _)
      .map {
        case ((dt, area, city, adId), count) =>
          CityClick(dt, area, city, adId, count)
      }

    //6. 更新MySQL数据库
    cityClick.foreachRDD(
      rdd =>
        rdd.foreachPartition {
          iter =>
            val connection: Connection = JDBCUtil.getConnection
            JDBCUtil.updateCityClick(connection, iter.toList)
            connection.close()
        }
    )

    //7. 最近十分钟的各个广告点击, 每一分钟展示一次
//    filteredOdsLog
//      .map(x => (x.adId, 1))
//      .window(Minutes(10), Seconds(15))
//      .reduceByKey(_ + _)
//      .print()

    //8. 文档的例子
    filteredOdsLog
      .map(x => ((timeFormat.format(new Date(x.timestamp)), x.adId), 1))
      .window(Minutes(2))
      .reduceByKey(_ + _)
      .map {
        case ((time, adId), count) => (adId, (time, count))
      }
      .groupByKey
      .mapValues(_.toList)
      .print()


    ssc.start()
    ssc.awaitTermination()
  }
}

case class UserAction(timestamp: Long, area: String, city: String, userId: Int, adId: Int)

case class UserClick(dt: String, userId: Int, adId: Int, count: Int)

case class CityClick(dt: String, area: String, city: String, adId: Int, count: Int)
