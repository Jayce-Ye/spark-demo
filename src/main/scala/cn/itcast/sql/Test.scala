package cn.itcast.sql
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

object Test {
  def main(args: Array[String]): Unit = {
   val spark: SparkSession = SparkSession.builder()
  .appName("spark").master("local[*]").getOrCreate()
    val sc: SparkContext = spark.sparkContext
    sc.setLogLevel("WARN")
    import spark.implicits._
    val df1 = Seq(
      ("SG", "CELL", 5000),
      ("SG", "PC", 15000),
      ("IN", "CELL", 4000),
      ("IN", "PC", 12000),
      ("SG", "CELL", 7000),
      ("SG", "PC", 25000),
      ("IN", "CELL", 7000),
      ("IN", "PC", 20000),
      ("SG", "CELL", 10000),
      ("SG", "PC", 35000),
      ("IN", "CELL", 9000),
      ("IN", "PC", 25000),
    ).toDF("COUNTRY", "PRODUCT", "REVENUE")
    df1.show()
    val df2 =  df1.groupBy("PRODUCT")
      .pivot("COUNTRY")
      .sum("REVENUE")
      .withColumnRenamed("IN", "INDIA")
      .withColumnRenamed("SG", "SINGAPORE")
    df2.show();

  }
}
