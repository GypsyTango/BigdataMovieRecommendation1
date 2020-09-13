package com.chen.test

import java.net.InetAddress

import com.mongodb.casbah.commons.MongoDBObject
import com.mongodb.casbah.{MongoClient, MongoClientURI}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsRequest
import org.elasticsearch.common.settings.Settings
import org.elasticsearch.common.transport.InetSocketTransportAddress
import org.elasticsearch.transport.client.PreBuiltTransportClient

/**
 * 初始化数据
 */
object DataLoader {

  // ---MongoDB 中的表 Collection---
  // Movie在MongoDB中的Collection名称
  val MOVIES_COLLECTION_NAME = "Movie"
  // Rating在MongoDB中的Collection名称
  val RATINGS_COLLECTION_NAME = "Rating"
  // Tag在MongoDB中的Collection名称
  val TAGS_COLLECTION_NAME = "Tag"
  // 正则表达式
  val ES_HOST_PORT_REGEX = "(.+):(\\d+)".r
  // ES type的名称
  val ES_TAG_TYPE_NAME = "Movie"

  def main(args: Array[String]): Unit = {
    // 全局配置封装
    val params = scala.collection.mutable.Map[String,Any]();
    // 全局路径封装
    val DATAFILE_MOVIES = "C:\\Users\\newwr\\Downloads\\reco_data\\small\\movies.csv"
    val DATAFILE_RATINGS = "C:\\Users\\newwr\\Downloads\\reco_data\\small\\ratings.csv"
    val DATAFILE_TAGS = "C:\\Users\\newwr\\Downloads\\reco_data\\small\\tags.csv"
    params += "spark.cores" -> "local"
    params += "mongo.uri" -> "mongodb://192.168.1.3:27017/recom"
    params += "mongo.db" -> "recom"
    // ES 封装
    params += "es.httpHosts" -> "192.168.1.3:9200"
    params += "es.transportHosts" -> "192.168.1.3:9300"
    params += "es.index" -> "recom"
    params += "es.cluster.name" -> "my-application"

    // 定义MongoDB配置对象
    implicit val mongoConfig = new MongoConfig(params("mongo.uri").asInstanceOf[String],
      params("mongo.db").asInstanceOf[String])
    // 定义ES配置对象
    implicit val esConfig = new ESConfig(
      params("es.httpHosts").asInstanceOf[String],
      params("es.transportHosts").asInstanceOf[String],
      params("es.index").asInstanceOf[String],
      params("es.cluster.name").asInstanceOf[String]
    )
    // 声明Spark配置
    val conf = new SparkConf().setAppName("DataLoader").setMaster(params("spark.cores").asInstanceOf[String]);
    // 声明Spark环境
    val spark = SparkSession.builder().config(conf).getOrCreate()
    // 加载数据集
    val movieRDD = spark.sparkContext.textFile(DATAFILE_MOVIES)
    val ratingRDD = spark.sparkContext.textFile(DATAFILE_RATINGS)
    val tagRDD = spark.sparkContext.textFile(DATAFILE_TAGS)
    // RDD转化成DataFrame
    import spark.implicits._
    // ---电影表---
    val moiveDF = movieRDD.map(
      line => {
        val x = line.split("\\^")
        Movie(x(0).trim.toInt, x(1).trim, x(2).trim, x(3).trim, x(4).trim,
          x(5).trim,x(6).trim,x(7).trim,x(8).trim,x(9).trim)
      }
    ).toDF()
//    moiveDF.show()
    // ---评分表---
    val ratingDF = ratingRDD.map(
      line => {
        val x = line.split(",")
        Rating(x(0).trim.toInt, x(1).trim.toInt, x(2).trim.toDouble, x(3).trim.toInt)
      }
    ).toDF()
//    ratingDF.show()
    // ---标签表---
    val tagDF = tagRDD.map(
      line => {
        val x = line.split(",")
        Tag(x(0).trim.toInt, x(1).trim.toInt, x(2).trim, x(3).trim.toInt)
      }
    ).toDF()
//    tagDF.show()

    // 数据写入MongoDB
//    storeDataInMongo(moiveDF, ratingDF, tagDF)

    // 数据保存到ES中
    // 将tag表中的tag字段用groupby("mid")的方法，将tag字段聚合到movie的表中，产生新的表，并传入到ES中
    /**
     * ---tag---
     * uid,       *mid*,      tag,               timestamp
     * 364,       318,        friendship,        1444529800
     * 364,       318,        Morgan Freeman,    1444529792
     * 364,       318,        narrated,          1444529829
     * 364,       318,        prison,            1444529824
     * ---movie---
     * *mid*：318
     * name：Shawshank Redemption, The (1994)
     * descr：Framed in the 1940s for the double murder of his wife and her lover, upstanding banker Andy Dufresne begins a new life at the Shawshank prison, where he puts his accounting skills to work for an amoral warden. During his long stretch in prison, Dufresne comes to be admired by the other inmates -- including an older prisoner named Red -- for his integrity and unquenchable sense of hope.
     * timelong：142 minutes
     * issue：December 21, 1999
     * shoot：1994
     * language：English
     * genres：Crime|Drama
     * actors：Tim Robbins|Morgan Freeman|Bob Gunton|Clancy Brown|Mark Rolston|James Whitmore|Gil Bellows|William Sadler|Jeffrey DeMunn|Larry Brandenburg|Neil Giuntoli|Brian Libby|David Proval|Joseph Ragno|Jude Ciccolella|Paul McCrane|Renee Blaine|Scott Mann|John Horton|Gordon Greene|Alfonso Freeman|V.J. Foster|Frank Medrano|Mack Miles|Gary Lee Davis|Ned Bellamy|Brian Delate|Don McManus|Dorothy Silver|Dion Anderson|Robert Haley|Bill Bolender|John R. Woodward|Rohn Thomas|Brian Brophy|Ken Magee|James Babson|Fred Culbertson|Alonzo F. Jones|Sergio Kato|Philip Ettington|Neil Summers|Tim Robbins|Morgan Freeman|Bob Gunton|Clancy Brown|Mark Rolston
     * directors：Frank Darabont
     */
    import org.apache.spark.sql.functions._
    // 1、缓存
    moiveDF.cache()
    tagDF.cache()
    // 2、聚合
    // （1）tags表
    val tagCollectDF = tagDF.groupBy("mid").agg(concat_ws("|",collect_set("tag")).as("tags"))
    // （2）tags合并到movie的表中
    val esMovieDF = moiveDF.join(tagCollectDF, Seq("mid","mid"), "left").select(
      "mid", "name", "descr", "timelong","issue", "shoot", "language", "genres", "actors", "directors", "tags"
    )
    esMovieDF.show()

    // 3、将esMovieDF放入ES
    storeDataInES(esMovieDF)

    // 用完DF后将其 unpersist
    moiveDF.unpersist()
    tagDF.unpersist()

    // 关掉 spark
    spark.close()
  }

  def storeDataInES(esMovieDF: DataFrame)(implicit esConfig : ESConfig) = {
    // 取要操作index的名称
    val indexName = esConfig.index
    // 连接ES配置
    val settings = Settings.builder().put("cluster.name", esConfig.clusterName).build()
    // 连接ES的客户端
    val esClient = new PreBuiltTransportClient(settings)
    // 设置IP，遍历所有节点
    esConfig.transportHosts.split(",")
      .foreach{

        case ES_HOST_PORT_REGEX(host : String, port : String) =>
          esClient.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(host),port.toInt))
      }

    // 判断如果Index存在，我们就删除它
    if(esClient.admin().indices().exists(new IndicesExistsRequest(indexName)).actionGet().isExists){
      // 存在index，删除
      esClient.admin().indices().delete(new DeleteIndexRequest(indexName)).actionGet()
    }

    // 创建index
    esClient.admin().indices().create(new CreateIndexRequest(indexName)).actionGet()

    val movieTypeName = s"$indexName/$ES_TAG_TYPE_NAME"
    val movieOption = Map("es.nodes" -> esConfig.httpHosts,
    "es.http.timeout" -> "100m",// 100 分钟
    "es.mapping.id" -> "mid")
    esMovieDF.write
      .options(movieOption)
      .mode("overwrite")
      .format("org.elasticsearch.spark.sql")
      .save(movieTypeName)
  }

  def storeDataInMongo(moiveDF: DataFrame, ratingDF: DataFrame, tagDF: DataFrame)(implicit mongoConfig: MongoConfig): Unit = {
    // 创建到MongoDB的连接
    val mongoClient = MongoClient(MongoClientURI(mongoConfig.uri))

    // ---删除MongoDB里面的数据库---
//    mongoClient(mongoConfig.db)(MOVIES_COLLECTION_NAME).dropCollection()
//    mongoClient(mongoConfig.db)(RATINGS_COLLECTION_NAME).dropCollection()
//    mongoClient(mongoConfig.db)(TAGS_COLLECTION_NAME).dropCollection()


    // ---写入MongoDB---
    // movie
    moiveDF.write
      .option("uri", mongoConfig.uri)
      .option("collection",MOVIES_COLLECTION_NAME)
      .mode("overwrite")
      .format("com.mongodb.spark.sql")
      .save()
    // rating
    ratingDF.write
      .option("uri", mongoConfig.uri)
      .option("collection",RATINGS_COLLECTION_NAME)
      .mode("overwrite")
      .format("com.mongodb.spark.sql")
      .save()
    // tag
    tagDF.write
      .option("uri", mongoConfig.uri)
      .option("collection",TAGS_COLLECTION_NAME)
      .mode("overwrite")
      .format("com.mongodb.spark.sql")
      .save()
    // ---创建索引---
    // movie
    mongoClient(mongoConfig.db)(MOVIES_COLLECTION_NAME).createIndex(MongoDBObject("mid" -> 1))
    // rating
    mongoClient(mongoConfig.db)(RATINGS_COLLECTION_NAME).createIndex(MongoDBObject("mid" -> 1))
    mongoClient(mongoConfig.db)(RATINGS_COLLECTION_NAME).createIndex(MongoDBObject("uid" -> 1))
    // tag
    mongoClient(mongoConfig.db)(TAGS_COLLECTION_NAME).createIndex(MongoDBObject("mid" -> 1))
    mongoClient(mongoConfig.db)(TAGS_COLLECTION_NAME).createIndex(MongoDBObject("uid" -> 1))
    // 关掉mongoCLient
    mongoClient.close()

  }

}
