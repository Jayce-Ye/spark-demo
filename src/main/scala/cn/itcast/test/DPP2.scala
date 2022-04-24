package cn.itcast.test

import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

/**
  * Author itcast
  * 动态分区裁剪（Dynamic Partition Pruning）
  */
object DPP2 {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder()
      .master("local[*]")
      .appName("DPP")
      .config("spark.sql.optimizer.dynamicPartitionPruning.enabled", "true")//默认为true
      .getOrCreate()
    val sc: SparkContext = spark.sparkContext
    sc.setLogLevel("WARN")

    import spark.implicits._

    spark.range(10000)
      .select($"id", $"id".as("k"))
      .write.partitionBy("k")
      .format("parquet")
      .mode("overwrite")
      .saveAsTable("tab1")

    spark.range(100)
      .select($"id", $"id".as("k"))
      .write.partitionBy("k")
      .format("parquet")
      .mode("overwrite")
      .saveAsTable("tab2")

    spark.sql("SELECT * FROM tab1 t1 JOIN tab2 t2 ON t1.k = t2.k AND t2.id < 2").explain()
    spark.sql("SELECT * FROM tab1 t1 JOIN tab2 t2 ON t1.k = t2.k AND t2.id < 2").show()

    Thread.sleep(Long.MaxValue)

    sc.stop()
    spark.stop()
  }
}