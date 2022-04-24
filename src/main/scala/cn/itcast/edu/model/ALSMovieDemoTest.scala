package cn.itcast.edu.model

import org.apache.spark.SparkContext
import org.apache.spark.ml.recommendation.{ALS, ALSModel}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

/**
 * Author itcast
 * Desc 
 */
object ALSMovieDemoTest {
  def main(args: Array[String]): Unit = {
    //TODO 0.准备环境
    val spark: SparkSession = SparkSession.builder().appName("BatchAnalysis").master("local[*]")
      .config("spark.sql.shuffle.partitions", "4")//本次测试时将分区数设置小一点,实际开发中可以根据集群规模调整大小,默认200
      .getOrCreate()
    val sc: SparkContext = spark.sparkContext
    sc.setLogLevel("WARN")
    import spark.implicits._
    import org.apache.spark.sql.functions._

    //TODO 1.加载数据并处理
    val fileDS: Dataset[String] = spark.read.textFile("data/input/u.data")
    val ratingDF: DataFrame = fileDS.map(line => {
      val arr: Array[String] = line.split("\t")
      (arr(0).toInt, arr(1).toInt, arr(2).toDouble)
    }).toDF("userId", "movieId", "score")

    val Array(trainSet,testSet) = ratingDF.randomSplit(Array(0.8,0.2))//按照8:2划分训练集和测试集

    //TODO 2.构建ALS推荐算法模型并训练
    val als: ALS = new ALS()
      .setUserCol("userId") //设置用户id是哪一列
      .setItemCol("movieId") //设置产品id是哪一列
      .setRatingCol("score") //设置评分列
      .setRank(10) //可以理解为Cm*n = Am*k X Bk*n 里面的k的值
      .setMaxIter(10) //最大迭代次数
      .setAlpha(1.0)//迭代步长

    //使用训练集训练模型
    val model: ALSModel = als.fit(trainSet)

    //使用测试集测试模型
    //val testResult: DataFrame = model.recommendForUserSubset(testSet,5)
    //计算模型误差--模型评估
    //......

    //TODO 3.给用户做推荐
    val result1: DataFrame = model.recommendForAllUsers(5)//给所有用户推荐5部电影
    val result2: DataFrame = model.recommendForAllItems(5)//给所有电影推荐5个用户

    val result3: DataFrame = model.recommendForUserSubset(sc.makeRDD(Array(196)).toDF("userId"),5)//给指定用户推荐5部电影
    val result4: DataFrame = model.recommendForItemSubset(sc.makeRDD(Array(242)).toDF("movieId"),5)//给指定电影推荐5个用户

    result1.show(false)
    result2.show(false)
    result3.show(false)
    result4.show(false)
  }
}
