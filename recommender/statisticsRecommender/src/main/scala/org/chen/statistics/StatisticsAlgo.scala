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

  // 3+4、按类别统计平均分最高的10个电影
  def genreTop10(spark: SparkSession, movies: Dataset[Movie])(implicit mongoConfig: MongoConfig): Unit = {
    // 定义出所有的电影类别
    val genres = List("Action","Adventure","Animation","Comedy","Crime","Documentary",
      "Drama","Family","Fantasy","Foreign","History","Horror","Music","Mystery","Romance",
      "Science","Tv","Thriller","War","Western")
    // 统计电影所有电影平均分
    val avgMovieScoreDF = spark.sql("select mid, avg(score) as avg from ratings group by mid").cache
    // 统计各电影类别中评分最高的前十部电影
    // movies表 和 avgMovieScoreDF表 进行join
    val moviesWithScoreDF = movies.join(avgMovieScoreDF,Seq("mid", "mid")).select("mid","avg","genres").cache
    // 将 genres list 转成 RDD
    val genresRDD = spark.sparkContext.makeRDD(genres)
    // 笛卡尔积操作,并进行筛选。
    // 如果genresRDD表里面的genres字段和moviesWithScoreDF表中的genres字段吻合，将本电影保存下来，其他的则予以删除
    import spark.implicits._
    val genresTopMovies = genresRDD.cartesian(moviesWithScoreDF.rdd).filter{
      case (genres, row) => {
        row.getAs[String]("genres").toLowerCase().contains(genres.toLowerCase())
      }
    }.map {
      // 更改格式为: RDD[(String, (Int, Double))]
      case (genres, row) => {
        (genres, ((row.getAs[Int]("mid")), row.getAs[Double]("avg")))
      }
    // 归类
    }.groupByKey()
      // 排序
      .map{
        case (genres, items) => {
          GenresRec(
            genres,
            items.toList.sortWith(_._2 > _._2).take(n = 10).map(x => Recommendation(x._1,x._2))
          )
        }
      }.toDF()

    // 写入AverageMoviesScore到mongoDB中
    avgMovieScoreDF.write
      .option("uri", mongoConfig.uri)
      .option("collection", AVERAGE_MOVIES_SCORE)
      .mode("overwrite")
      .format("com.mongodb.spark.sql")
      .save()

    // 写入GenresTopMovies到mongoDB中
    genresTopMovies.write
      .option("uri", mongoConfig.uri)
      .option("collection", GENRES_TOP_MOVIES)
      .mode("overwrite")
      .format("com.mongodb.spark.sql")
      .save()

  }
}
