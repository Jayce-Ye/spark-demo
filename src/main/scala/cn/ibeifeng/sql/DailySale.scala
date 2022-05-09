package cn.ibeifeng.sql

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.{Encoder, Row, SQLContext}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.functions._



/**
 * @author Administrator
 */
object DailySale {
  
  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf()
        .setMaster("local")
        .setAppName("DailySale")
    val sc: SparkContext = new SparkContext(conf)
    val sqlContext:SQLContext = new SQLContext(sc)

    import sqlContext.implicits._
    
    // 说明一下，业务的特点
    // 实际上呢，我们可以做一个，单独统计网站登录用户的销售额的统计
    // 有些时候，会出现日志的上报的错误和异常，比如日志里丢了用户的信息，那么这种，我们就一律不统计了
    
    // 模拟数据
    val userSaleLog = Array("2015-10-01,55.05,1122",
        "2015-10-01,23.15,1133",
        "2015-10-01,15.20,",
        "2015-10-02,56.05,1144",
        "2015-10-02,78.87,1155",
        "2015-10-02,113.02,1123")
    val userSaleLogRDD = sc.parallelize(userSaleLog, 5)
    
    // 进行有效销售日志的过滤
    val filteredUserSaleLogRDD = userSaleLogRDD
        .filter { log => if (log.split(",").length == 3) true else false }
      
    val userSaleLogRowRDD = filteredUserSaleLogRDD
        .map { log => Row(log.split(",")(0), log.split(",")(1).toDouble) }
    
    val structType = StructType(Array(
        StructField("date", StringType, true),
        StructField("sale_amount", DoubleType, true)))
    
    val userSaleLogDF = sqlContext.createDataFrame(userSaleLogRowRDD, structType);
//    implicit val encoder: Encoder[Row] =org.apache.spark.sql.Row//添加字符串类型编码器
    // 开始进行每日销售额的统计
//    implicit  val encoder: Encoder[org.apache.spark.sql.Row]

    userSaleLogDF.groupBy("date")
        .agg('date, sum('sale_amount)).rdd
        .map { row => Row(row(1), row(2)) }
        .collect()
        .foreach(println)  
  }
  
}