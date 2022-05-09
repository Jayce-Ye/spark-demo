package cn.ibeifeng.spark.sql

import java.io.File

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.{SaveMode, SparkSession}

/**
 * @author Administrator
 */
object HiveDataSource {
  
  def main(args: Array[String]): Unit = {
//    val conf = new SparkConf()
//        .setAppName("HiveDataSource");
//    val sc = new SparkContext(conf);
//    val hiveContext = new HiveContext(sc);
val warehouseLocation = new File("spark-warehouse").getAbsolutePath
    val spark = SparkSession
  .builder()
  .appName("Spark Hive Example")
  .config("spark.sql.warehouse.dir", warehouseLocation)
  .enableHiveSupport()
  .getOrCreate()
    import spark.implicits._
    import spark.sql

    sql("DROP TABLE IF EXISTS student_infos");
    sql("CREATE TABLE IF NOT EXISTS student_infos (name STRING, age INT)");
    sql("LOAD DATA "
        + "LOCAL INPATH '/usr/local/spark-study/resources/student_infos.txt' "
        + "INTO TABLE student_infos");
    
    sql("DROP TABLE IF EXISTS student_scores");
    sql("CREATE TABLE IF NOT EXISTS student_scores (name STRING, score INT)");
    sql("LOAD DATA "
        + "LOCAL INPATH '/usr/local/spark-study/resources/student_scores.txt' "
        + "INTO TABLE student_scores");
    
    val goodStudentsDF = sql("SELECT si.name, si.age, ss.score "
        + "FROM student_infos si "
        + "JOIN student_scores ss ON si.name=ss.name "
        + "WHERE ss.score>=80");

    sql("DROP TABLE IF EXISTS good_student_infos");
    goodStudentsDF.write.mode(SaveMode.Overwrite).saveAsTable("good_student_infos");
    
    sql("SELECT * FROM good_student_infos").show()

//    for(goodStudentRow <- goodStudentRows) {
//      println(goodStudentRow);
//    }
  }
  
}