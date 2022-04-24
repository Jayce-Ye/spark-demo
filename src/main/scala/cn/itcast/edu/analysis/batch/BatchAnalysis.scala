package cn.itcast.edu.analysis.batch

import cn.itcast.edu.bean.AnswerWithRecommendations
import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

/**
 * Author itcast
 * Desc 离线分析学生学习情况
 */
object BatchAnalysis {
  def main(args: Array[String]): Unit = {
    //TODO 0.准备环境-SparkSession
    val spark: SparkSession = SparkSession.builder().appName("BatchAnalysis").master("local[*]")
      .config("spark.sql.shuffle.partitions", "4")//本次测试时将分区数设置小一点,实际开发中可以根据集群规模调整大小,默认200
      .getOrCreate()
    val sc: SparkContext = spark.sparkContext
    sc.setLogLevel("WARN")
    import spark.implicits._
    import org.apache.spark.sql.functions._

    //TODO 1.加载数据-MySQL
    val properties = new java.util.Properties()
    properties.setProperty("user", "root")
    properties.setProperty("password", "root")
    val allInfoDS: Dataset[AnswerWithRecommendations] = spark.read.jdbc(
      "jdbc:mysql://localhost:3306/edu?useUnicode=true&characterEncoding=utf8",
      "t_recommended",
      properties
    ).as[AnswerWithRecommendations]

    //TODO 2.处理数据/分析数据
    //TODO ===SQL
    //TODO: 需求:1.各科目热点题分析
    // 找到Top50热点题对应的科目，然后统计这些科目中，分别包含这几道热点题的条目数
    /*
    题号 热度
    1    10
    2    9
    3    8
     */
    /*
    题号 热度 科目
    1    10   语文
    2    9    数学
    3    8    数学
     */
    /*
    科目 热点题数量
    语文  1
    数学  2
     */
    //1.统计Top50热点题--子查询t1
    //2.将t1和原始表t_answer关联,并按学科分组聚合统计各个学科包含的热点题的数量
    //==================写法1:SQL风格================
    /*spark.sql(
      """SELECT
        |  subject_id, count(t_answer.question_id) AS hot_question_count
        | FROM
        |  (SELECT
        |    question_id, count(*) AS frequency
        |  FROM
        |    t_answer
        |  GROUP BY
        |    question_id
        |  ORDER BY
        |    frequency
        |  DESC LIMIT
        |    50) t1
        |JOIN
        |  t_answer
        |ON
        |  t1.question_id = t_answer.question_id
        |GROUP BY
        |  subject_id
        |ORDER BY
        |  hot_question_count
        | DESC
  """.stripMargin)
      .show()*/

    //TODO: 需求:2.各科目推荐题分析
    // 找到Top20热点题对应的推荐题目，然后找到推荐题目对应的科目，并统计每个科目分别包含推荐题目的条数
    /*
    科目,包含的推荐的题目的数量
    英语,105
    语文,95
    数学,89
     */
    //1.统计热点题Top20--子查询t1
    //2.将t1和原始表t_answer关联,得到热点题Top20的推荐题列表t2
    //3.用SPLIT将recommendations中的ids用","切为数组,然后用EXPLODE将列转行,并记为t3
    //4.对推荐的题目进行去重,将t3和t_answer原始表进行join,得到每个推荐的题目所属的科目,记为t4
    //5.统计各个科目包含的推荐的题目数量并倒序排序(已去重)
    //==================写法1:SQL风格================
    /*spark.sql(
      """SELECT
        |    t4.subject_id,
        |    COUNT(*) AS frequency
        |FROM
        |    (SELECT
        |        DISTINCT(t3.question_id),
        |        t_answer.subject_id
        |     FROM
        |       (SELECT
        |           EXPLODE(SPLIT(t2.recommendations, ',')) AS question_id
        |        FROM
        |            (SELECT
        |                recommendations
        |             FROM
        |                 (SELECT
        |                      question_id,
        |                      COUNT(*) AS frequency
        |                  FROM
        |                      t_answer
        |                  GROUP BY
        |                      question_id
        |                  ORDER BY
        |                      frequency
        |                  DESC LIMIT
        |                      20) t1
        |             JOIN
        |                 t_answer
        |             ON
        |                 t1.question_id = t_answer.question_id) t2) t3
        |      JOIN
        |         t_answer
        |      ON
        |         t3.question_id = t_answer.question_id) t4
        |GROUP BY
        |    t4.subject_id
        |ORDER BY
        |    frequency
        |DESC
  """.stripMargin)
      .show*/

    //TODO ===DSL
    //TODO: 需求:1.各科目热点题分析
    // 找到Top50热点题对应的科目，然后统计这些科目中，分别包含这几道热点题的条目数
    /*
    题号 热度
    1    10
    2    9
    3    8

    题号 热度 科目
    1    10   语文
    2    9    数学
    3    8    数学

    科目 热点题数量
    语文  1
    数学  2
     */
    //1.统计Top50热点题--子查询t1
    val hotTop50: Dataset[Row] = allInfoDS.groupBy('question_id)
      .agg(count("*") as "hot")
      .orderBy('hot.desc)
      .limit(50)
    //2.将t1和原始表t_answer关联,得到热点题对应的科目
    val joinDF: DataFrame = hotTop50.join(allInfoDS.dropDuplicates("question_id"),"question_id")
    //3.按学科分组聚合统计各个学科包含的热点题的数量
    val result1: Dataset[Row] = joinDF.groupBy('subject_id)
      .agg(count("*") as "hotCount")
      .orderBy('hotCount.desc)

    //TODO: 需求:2.各科目推荐题分析
    // 找到Top20热点题对应的推荐题目，然后找到推荐题目对应的科目，并统计每个科目分别包含推荐题目的条数
    /*
    题号  热度
    1     10
    2     9
    题号  热度  推荐题
    1     10    2,3,4
    2     9     3,4,5
    推荐题 科目
    2      数学
    3      数学
    4      物理
    5      化学
    科目  推荐题数量
    数学  2
    物理  1
    化学  1
     */
    //1.统计热点题Top20--子查询t1
    val hotTop20: Dataset[Row] = allInfoDS.groupBy('question_id)
      .agg(count("*") as "hot")
      .orderBy('hot.desc)
      .limit(20)
    //2.将t1和原始表t_answer关联,得到热点题Top20的推荐题列表t2
    val ridsDF: DataFrame = hotTop20.join(allInfoDS, "question_id")
      .select("recommendations")

    //3.用SPLIT将recommendations中的ids用","切为数组,然后用EXPLODE将列转行,并记为t3
    val ridsDS: Dataset[Row] = ridsDF.select(explode(split('recommendations, ",")) as "question_id")
      .dropDuplicates("question_id")
    //4.对推荐的题目进行去重,将t3和t_answer原始表进行join,得到每个推荐的题目所属的科目,记为t4
    //df1.join(df2, $"df1Key" === $"df2Key")
    //df1.join(df2).where($"df1Key" === $"df2Key")
    val ridAndSid: DataFrame = ridsDS.join(allInfoDS.dropDuplicates("question_id"),"question_id")
    //5.统计各个科目包含的推荐的题目数量并倒序排序(已去重)
    val result2: Dataset[Row] = ridAndSid.groupBy('subject_id)
      .agg(count("*") as "rcount")
      .orderBy('rcount.desc)

    //TODO 3.输出结果
    //result1.show()
    result2.show()

    //TODO 4.关闭资源
    spark.stop()
  }
}
