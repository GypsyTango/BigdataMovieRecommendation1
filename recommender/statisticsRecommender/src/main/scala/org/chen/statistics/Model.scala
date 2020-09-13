package org.chen.statistics

/**
 * ===电影表===
 * mid：1
 * name：Toy Story (1995)
 * descr：空
 * timelong：81 minutes
 * issue：March 20, 2001
 * shoot：1995
 * language：English
 * genres：Adventure|Animation|Children|Comedy|Fantasy
 * actors：Tom Hanks|Tim Allen|Don Rickles|Jim Varney|Wallace Shawn|John Ratzenberger|Annie Potts|John Morris|Erik von Detten|Laurie Metcalf|R. Lee Ermey|Sarah Freeman|Penn Jillette|Tom Hanks|Tim Allen|Don Rickles|Jim Varney|Wallace Shawn
 * directors：John Lasseter
 */
case class Movie(val mid : Int, val name : String, val descr : String, val timelong : String,
                 val issue : String, val shoot : String, val language : String, val genres : String,
                 val actors : String, val directors : String)

/**
 * ===评分表===
 * uid：1
 * mid：31
 * score：2.5
 * timestamp：1260759144
 */
case class Rating(val uid : Int, val mid : Int, val score : Double, val timestamp : Int)

/**
 * ===标签表===
 * uid：15
 * mid：339
 * tag：sandra 'boring' bullock
 * timestamp：1138537770
 */
case class Tag(val uid : Int, val mid : Int, val tag : String, val timestamp : Int)

/**
 * mongoDB配置对象
 * uri：地址
 * db：数据库
 */
case class MongoConfig(val uri : String, val db : String)

/**
 * ES的配置对象
 * httpHosts：地址
 * transportHosts：保存所有ES节点信息
 * index：索引
 * clusterName：集群名称
 */
case class ESConfig(val httpHosts : String, val transportHosts : String, val index : String, val clusterName : String)









