package org.chen.statistics
import java.text.SimpleDateFormat
import java.util.Date

import org.apache.spark.sql.{Dataset, SparkSession}

// 离线统计的方法实现
object StatisticsAlgo {

  // 0、定义常量
  val RATE_MOST_MOVIES = "RateMostMovies"
  val RATE_MOST_RECENT_MOVIES = "RateMostRecentMovies"
  val AVERAGE_MOVIES_SCORE = "AverageMoviesScore"
  val GENRES_TOP_MOVIES = "GenresTopMovies"

  // 1、统计最多被评分的电影
  def rateMost(spark:SparkSession)(implicit mongoConfig: MongoConfig): Unit = {
    // 统计出每个电影的评分次数，降序
    val rateMostDF = spark.sql("select mid, count(1) as count from ratings group by mid order by count desc")
    rateMostDF.write
      .option("uri", mongoConfig.uri)
      .option("collection", RATE_MOST_MOVIES)
      .mode("overwrite")
      .format("com.mongodb.spark.sql")
      .save()
  }

  // 2、统计热门电影
  def rateMostRecentMovies(spark: SparkSession)(implicit mongoConfig: MongoConfig): Unit = {
    // udf 作用：1260759144 转换成 "202010" 再转换成 202010L
    val simpleDateFormat = new SimpleDateFormat("yyyyMM")
    spark.udf.register("changeDate", (x:Long) => simpleDateFormat.format(new Date(x*1000L)).toLong)
    // spark sql
    val yearMonthOfRatings = spark.sql("select mid, uid, score, changeDate(timestamp) as yearmonth from ratings")
    yearMonthOfRatings.createOrReplaceTempView("ymRatings")
    spark.sql("select mid, count(1) as count, yearmonth from ymRatings group by mid, yearmonth order by yearmonth desc").write
      .option("uri", mongoConfig.uri)
      .option("collection", RATE_MOST_RECENT_MOVIES)
      .mode("overwrite")
      .format("com.mongodb.spark.sql")
      .save()
  }

  // 3、按类别统计平均分最高的10个电影
  def genreTop10(spark: SparkSession, movies: Dataset[Movie])(implicit mongoConfig: MongoConfig): Unit = {

  }
}
