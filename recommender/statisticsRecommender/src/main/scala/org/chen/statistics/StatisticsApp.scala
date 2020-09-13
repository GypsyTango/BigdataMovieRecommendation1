package org.chen.statistics

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

// 离线统计的main方法
/**
 * 数据流程:
 * spark读取mongodb的数据，离线统计后将结果写入mongodb
 * 统计值:
 * 1、评分个数最多的电影:
 * 获取所有历史数据中，评分个数最多的电影集合，统计出每个电影的评分个数 -> RateMostMovies
 * 2、近期热门电影:
 * 按照月份来统计，本月评分最多的电影，被认为是热门电影 -> 统计每月每个电影的的评分总量 -> RateMostRecentMovies
 * 3、电影的平均得分
 * 把每个电影的用户评分进行平均计算，得出每个电影的平均得分 -> AverageMoviesScore
 * 4、统计每种类别电影的Top10
 * 将每种类别的电影中，评分最高的n个电影计算出来 -> GenresTopMovies
 */

object StatisticsApp extends App {
  // 定义常量
  val RATING_COLLECTION_NAME = "Rating"
  val MOVIE_COLLECTION_NAME = "Movie"

  // 参数
  val params = scala.collection.mutable.Map[String, Any]()
  params += "spark.cores" -> "local[2]"
  params += "mongo.uri" -> "mongodb://192.168.1.3:27017/recom"
  params += "mongo.db" -> "recom"

  val conf = new SparkConf().setAppName("StatisticApp").setMaster(params("spark.cores").asInstanceOf[String])
  val spark = SparkSession.builder().config(conf).getOrCreate()
  // 初始化mongo config对象
  import spark.implicits._
  implicit val mongoConfig = new MongoConfig(params("mongo.uri").asInstanceOf[String],
    params("mongo.db").asInstanceOf[String])
  // 读mongodb数据
  val ratings = spark.read
    .option("uri", mongoConfig.uri)
    .option("collection", RATING_COLLECTION_NAME)
    .format("com.mongodb.spark.sql")
    .load()
    .as[Rating].cache

  val movies = spark.read
    .option("uri", mongoConfig.uri)
    .option("collection", MOVIE_COLLECTION_NAME)
    .format("com.mongodb.spark.sql")
    .load()
    .as[Movie].cache

  ratings.createOrReplaceTempView("ratings")
  // ====统计====
  // 1、电影被评分最多次的电影
//  StatisticsAlgo.rateMost(spark)
//  ratings.unpersist()
//  movies.unpersist()
//  spark.close()
//  // 2、近期热门电影
//  StatisticsAlgo.rateMostRecentMovies(spark)
//  ratings.unpersist()
//  movies.unpersist()
//  spark.close()
  //  3+4、按类别统计平均分最高的10个电影
  StatisticsAlgo.genreTop10(spark, movies)
  ratings.unpersist()
  movies.unpersist()
  spark.close()
}
