package com.atguigu.util

import com.alibaba.druid.pool.DruidDataSourceFactory
import com.atguigu.app.{CityClick, UserClick}

import java.sql.{Connection, PreparedStatement, ResultSet}
import java.util.Properties
import javax.sql.DataSource
import scala.collection.mutable
import scala.collection.mutable.ListBuffer

object JDBCUtil {
  val dataSource: DataSource = init()

  //初始化连接池方法
  def init(): DataSource = {

    val properties = new Properties()
    val config: Properties = PropertiesUtil.load("config.properties")

    properties.setProperty("driverClassName", "com.mysql.jdbc.Driver")
    properties.setProperty("url", config.getProperty("jdbc.url"))
    properties.setProperty("username", config.getProperty("jdbc.user"))
    properties.setProperty("password", config.getProperty("jdbc.password"))
    properties.setProperty("maxActive", config.getProperty("jdbc.datasource.size"))

    DruidDataSourceFactory.createDataSource(properties)
  }


  /**
   * 返回一个数据库连接
   *
   * @return 一个数据库连接
   */
  def getConnection: Connection = {
    dataSource.getConnection
  }

  /**
   * 获取黑名单用户
   *
   * @param connection JDBC连接
   * @return 黑名单用户集合
   */
  def getBlackList(connection: Connection): List[Int] = {
    val preparedStatement: PreparedStatement = connection.
      prepareStatement("select userid from spark2020.black_list")
    val resultSet: ResultSet = preparedStatement.executeQuery()
    val listBuffer: ListBuffer[Int] = mutable.ListBuffer.empty[Int]
    while (resultSet.next()) {
      listBuffer += resultSet.getInt(1)
    }
    listBuffer.toList
  }

  def updateUserClick(connection: Connection, userClicks: List[UserClick]): Unit = {
    val preparedStatement: PreparedStatement = connection
      .prepareStatement(
        """
          |insert into spark2020.user_ad_count (dt, userid, adid, count)
          |values(?,?,?,?)
          |on duplicate key update count = count + ?
          |""".stripMargin)
    for (UserClick(dt, userId, adId, count) <- userClicks) {
      preparedStatement.setObject(1, dt)
      preparedStatement.setObject(2, userId)
      preparedStatement.setObject(3, adId)
      preparedStatement.setObject(4, count)
      preparedStatement.setObject(5, count)
      preparedStatement.executeUpdate()
    }
    preparedStatement.close()
  }

  def getNewBlackList(connection: Connection): List[Int] = {
    val preparedStatement: PreparedStatement = connection.prepareStatement("select userid from spark2020.user_ad_count where count>3000")
    val resultSet: ResultSet = preparedStatement.executeQuery()
    val listBuffer: ListBuffer[Int] = mutable.ListBuffer.empty[Int]
    while (resultSet.next()) {
      listBuffer += resultSet.getInt(1)
    }
    listBuffer.toList
  }

  def updateBlackList(connection: Connection, blackList: List[Int]): Unit = {
    val preparedStatement: PreparedStatement = connection
      .prepareStatement("insert ignore into spark2020.black_list(userid) values (?)")
    for (userId <- blackList) {
      preparedStatement.setObject(1, userId)
      preparedStatement.executeUpdate()
    }
    preparedStatement.close()
  }

  def updateCityClick(connection: Connection, cityClicks: List[CityClick]): Unit = {
    val preparedStatement: PreparedStatement = connection
      .prepareStatement(
        """
          |insert into spark2020.area_city_ad_count
          |(dt, area, city, adid, count)
          |values (?,?,?,?,?)
          |on duplicate key update count = count + ?
          |""".stripMargin)
    for (CityClick(dt, area, city, adId, count) <- cityClicks) {
      preparedStatement.setObject(1, dt)
      preparedStatement.setObject(2, area)
      preparedStatement.setObject(3, city)
      preparedStatement.setObject(4, adId)
      preparedStatement.setObject(5, count)
      preparedStatement.setObject(6, count)
      preparedStatement.executeUpdate()
    }
    preparedStatement.close()
  }

  def main(args: Array[String]): Unit = {
    val userClicks = List(
      UserClick("2020-06-14", 1, 1, 20),
      UserClick("2020-06-14", 1, 2, 20),
      UserClick("2020-06-14", 1, 2, 20),
    )
    updateUserClick(getConnection, userClicks)
  }

}
