package com.hnust.movie

import java.sql.{Connection, DriverManager, PreparedStatement, ResultSet}
import java.util
import java.util.{Collections, UUID}

import com.alibaba.fastjson.serializer.SerializerFeature
import com.alibaba.fastjson.{JSON, JSONObject}
import com.hnust.movie.entity.CaseClasses.{Mid2Score, Mid2SimilarScore, MovieInfo_DB, MovieResc}
import com.hnust.movie.entity.{CaseClasses, Constant}
import com.hnust.movie.utils.DateUtil
import com.mongodb.{BasicDBList, BasicDBObject}
import com.mongodb.casbah.commons.MongoDBObject
import com.mongodb.casbah.{MongoClient, MongoCollection, MongoCursor, MongoDB}
import org.apache.commons.lang.StringUtils
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import org.json4s.jackson.Serialization
import redis.clients.jedis.Jedis

import scala.collection.mutable
import scala.collection.mutable.{ArrayBuffer, ListBuffer}

/**
  * @Title:实时推荐
  * @Author: ggh
  * @Date: 2020/6/12 10:20
  */
object RealTimeRecommend {


  def main(args: Array[String]): Unit = {


    //-Xms4000m -Xmx4000m
    val sparkConf: SparkConf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("RealTimeRecommend")
    //      .set("spark.executor.memory","3800m")
    //      .set("spark.storage.storageFraction","3048m")
    //      .set("spark.locality.wait","60")
    //      .set("spark.rdd.compress","true")
    //      .set("spark.driver.memory","2048m")
    //      .set("spark.memory.storageFraction","")


    val spark: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()
    val sparkContext: SparkContext = spark.sparkContext

    val ssc = new StreamingContext(sparkContext, Seconds(2))


    val kafkaParams = Map(
      "bootstrap.servers" -> "192.168.177.103:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "rating",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (true: java.lang.Boolean)
    )

    //创建Dstream流
    val kafkaStream: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream[String, String](ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](Array("rating"), kafkaParams)
    )

    //评分流：uid|mid|rating|date
    //进行转换=>(uid,mid,rating)
    val ratingDstream = kafkaStream.map {
      rating => {
        println(rating)
        var splits: Array[String] = null;
        try {
          splits = rating.value().split("\\|")

        } catch {
          case ex: Exception => {
            println("获取到的数据格式非法")
          }
        }
        (splits(0).toLong, splits(1).toLong, splits(2))

      }
    }

    //    val mids: DStream[Long] = ratingDstream.map(_._1)

    //2774532135|1765009|80|2017-03-14

    //按分区来进行遍历，在每一个分区里获取一个mongo连接
    ratingDstream.foreachRDD(_.foreachPartition {
      ratings => {
        //获取mongo连接
        val mongoClient = MongoClient("localhost")
        val mongoDB: MongoDB = mongoClient(Constant.MONGODB_NAME)
        val collection: MongoCollection = mongoDB(Constant.MID_2_SIMILAR_SCORE)
        val userRescCollection: MongoCollection = mongoDB(Constant.USER_RECOMMENDATION_MONGODB)

        val jedis: Jedis = new Jedis("localhost")


        ratings.foreach {
          rating => {
            println("接收到数据:" + rating)

            //创建一个map，封装一个电影和另外其他电影的的相似度
            var mid2ScoreMap = new mutable.HashMap[Long, Double]()

            //1.从mongo获取该电影的电影相似度列表
            val bObject = MongoDBObject("mid" -> rating._2)
            val cursorType: MongoCursor = collection.find(bObject)

            if (cursorType.hasNext) {
              val next: _root_.com.mongodb.casbah.Imports.DBObject = cursorType.next()

              val list: util.List[Mid2Score] = JSON.parseArray(next.get("mid2Score").toString, classOf[Mid2Score])

              val iterator: util.Iterator[Mid2Score] = list.iterator()
              while (iterator.hasNext) {
                val mid2Score: Mid2Score = iterator.next()
                mid2ScoreMap += ((mid2Score.mid, mid2Score.score))
              }

              //获取数据库连接
              val connection: Connection = getConnection()
              //存放用户最近的几次评分：(电影id，评分值)
              val recentRating: ListBuffer[(Long, Double)] = ListBuffer()

              //获取与该电影最相似的前30个电影和用户最近的几次评分
              val topSimilarMoviesArray: Array[(Long, Double)] = getTopSimilarMovies(30, rating._1, mid2ScoreMap, connection, recentRating)
//              topSimilarMoviesArray.foreach(println(_))

              println("============================")
              //2.获取该用户最近的几次评分
//              recentRating.foreach(println(_))

              //3.计算每一个待推荐电影的推荐优先级
              val movieLevelArray: Array[(Long, Double)] = computeRecommendedLevel(topSimilarMoviesArray, recentRating, mid2ScoreMap, collection)

              print("driver:")
              movieLevelArray.foreach(println(_))

              //4.将计算出的电影推荐优先级结果保存到mongodb
              saveToMongo(rating._1, movieLevelArray, userRescCollection, jedis, connection)


              //              println(mid2ScoreMap)
              //              println(mid2ScoreMap.get(1291562).get)

            }

          }
        }

        //关闭连接
        mongoClient.close()
        jedis.close()

      }
    })


    ssc.start()
    println("开始收集数据")
    ssc.awaitTermination()

  }


  /**
    * @title:
    * @description: 保存数据到mongodb
    * @param: uid:用户id
    * @param: movieLevelArray：用户的电影推荐列表
    * @param: userRescCollection：mongo连接
    * @param: jedis：redis连接
    * @author:ggh
    * @updateTime: 2020/6/16 10:00
    **/
  def saveToMongo(uid: Long,
                  movieLevelArray: Array[(Long, Double)],
                  userRescCollection: MongoCollection,
                  jedis: Jedis,
                  connection: Connection
                 ) = {

    val date: String = DateUtil.getCurrentTime("yyyy-MM-dd")

//    movieLevelArray.foreach(println(_))

//    getInfoFromCache(1765009, jedis, connection)

    //从缓存中获取每个电影的详情数据，并封装成MovieResc
    val movieRescs: Array[MovieResc] = movieLevelArray.map {
      x => {
        getInfoFromCache(x._1, jedis, connection)
      }
    }.filter(x => x != null)

//    movieRescs.foreach(println(_))

    //保存到mongodb
    userRescCollection.insert(MongoDBObject("date" -> date, "uid" -> uid,
      "userResc" -> movieRescs.map(x => MongoDBObject("mid"->x.mid,"movieName"->x.movieName,"hotDegree"->x.hotDegree,"imgUrls"->x.imgUrls,"ratingNum"->x.ratingNum,"categories"->x.categories,"score"->0.0))))
//      "userResc" -> movieRescs.map(x => MongoDBObject("mid"->x.mid,"movieName"->x.movieName,"hotDegree"->x.hotDegree,"imgUrls"->x.imgUrls,"ratingNum"->x.ratingNum,"categories"->x.categories))))

    println("保存完毕")

  }

  /**
    * @title:
    * @description: 从缓存中获取电影的详情数据
    * @param: mid
    * @param: jedis
    * @param: connection:数据库连接
    * @author:ggh
    * @updateTime: 2020/6/16 10:24
    **/
  def getInfoFromCache(mid: Long, jedis: Jedis, connection: Connection): MovieResc = {

//    println("进入缓存方法")

    var movieInfoStr: String = ""

    //先从缓存里面获取
    movieInfoStr = jedis.get("movie:" + mid + ":info")

    if (StringUtils.isNotBlank(movieInfoStr)) {
      //如果获取到了数据

      //解析数据
      val movieInfo_DB: MovieInfo_DB = JSON.parseObject(movieInfoStr, classOf[MovieInfo_DB])

      //设置过期时间
      jedis.expire("movie:" + mid + ":info", 10 * 60)

      return MovieResc(movieInfo_DB.mid, movieInfo_DB.movie_name, movieInfo_DB.hot_degree, movieInfo_DB.img_urls, movieInfo_DB.rating_num, movieInfo_DB.categories, 0.0)

    } else {
      //没有从缓存中获取到数据，就要从数据库中获取

      val token = UUID.randomUUID().toString

      //尝试获取锁
      val lockKey = "movie:" + mid + ":lock"
      val lockResult: String = jedis.set(lockKey, token, "NX", "PX", 1000 * 30)

      if ("OK".equals(lockResult)) {
        //获取到了锁，就允许从数据库获取数据
        val sql = "SELECT * FROM m_movie_info WHERE mid=? AND off_shelf=0"
        val ps: PreparedStatement = connection.prepareStatement(sql)

        ps.setLong(1, mid)

        val rs: ResultSet = ps.executeQuery()

        if (rs.next()) {
          val mid: Long = rs.getLong("mid")
          val movieName: String = rs.getString("movie_name")
          val imgUrls: String = rs.getString("img_urls")
          val directors: String = rs.getString("directors")
          val screenWriter: String = rs.getString("screen_writer")
          val mainActors: String = rs.getString("main_actors")
          val categories: String = rs.getString("categories")
          val location: String = rs.getString("location")
          val language: String = rs.getString("language")
          val releaseDate: String = rs.getString("release_date")
          val runTime: String = rs.getString("run_time")
          val alias: String = rs.getString("alias")
          val summery: String = rs.getString("summery")
          val ratingNum: Float = rs.getFloat("rating_num")
          val offShelf: Int = rs.getInt("off_shelf")
          val hotDegree: Int = rs.getInt("hot_degree")
          val viewNum: Long = rs.getLong("view_num")
          val playUrl: String = rs.getString("play_url")

          val movieInfo: MovieInfo_DB = MovieInfo_DB(mid, movieName, imgUrls, directors, screenWriter, mainActors, categories, location, language, releaseDate, runTime, alias, summery, ratingNum, offShelf, hotDegree, viewNum, playUrl)


          //转换成json字符串
          implicit val formats = org.json4s.DefaultFormats
          val movieJsonStr: String = Serialization.write(movieInfo)

          //设置到缓存中
          jedis.setex("movie:" + mid + ":info", 10 * 60, movieJsonStr)

          //释放锁
          //lua脚本
          val script = "if redis.call('get', KEYS[1])==ARGV[1] then return redis.call('del', KEYS[1]) else return 0 end"
          //在获取值的时候获取到了就直接删除
          jedis.eval(script, Collections.singletonList(lockKey), Collections.singletonList(token))

          //返回结果
          return MovieResc(mid, movieName, hotDegree, imgUrls, ratingNum, categories, 0.0)
        } else {
          //如果没有从数据库获取到数据,将空值设置进缓存
          jedis.setex("movie:" + mid + ":info", 60 * 3, "")

          //释放锁
          //lua脚本
          val script = "if redis.call('get', KEYS[1])==ARGV[1] then return redis.call('del', KEYS[1]) else return 0 end"

          //在获取值的时候获取到了就直接删除
          jedis.eval(script, Collections.singletonList(lockKey), Collections.singletonList(token));

          //返回一个空
          return null
        }

      } else {
        //如果没有获取到锁，就等待,新调用自己

        return getInfoFromCache(mid, jedis, connection)
      }

    }

  }


  /**
    * @title:
    * @description: 获取mid1和mid2之间的相似度
    * @param: mid1
    * @param: mid2
    * @author:ggh
    * @updateTime: 2020/6/14 21:54
    **/
  def getSimSore(mid1: Long, mid2: Long, collection: MongoCollection) = {

    //    MongoDBObject("mid"->mid1,"mid2Score.mid"->mid2)

    //获取电影id为mid1的数据
    val bObject = MongoDBObject("mid" -> mid1)
    val cursorType: MongoCursor = collection.find(bObject)

    if (cursorType.hasNext) {
      val next: _root_.com.mongodb.casbah.Imports.DBObject = cursorType.next()

      val mid2ScoresList: util.List[Mid2Score] = JSON.parseArray(next.get("mid2Score").toString, classOf[Mid2Score])

      //过滤出与第一id为mid1的相似度列表中电影id为mid2的数据
      val filteredArray: Array[Mid2Score] = mid2ScoresList.toArray.map(_.asInstanceOf[Mid2Score]).filter(_.mid == mid2)
      //      val filteredArray: Array[AnyRef] = mid2ScoresList.toArray.filter(x => {
      //        val mid2Score: Mid2Score = x.asInstanceOf[Mid2Score]
      //        mid2Score.mid == mid2
      //      })

      //获取相似度
      if (filteredArray.size > 0) {
        filteredArray(0).score
      } else {
        0.0
      }

    }

    0.0

  }

  /**
    * @title:
    * @description: 计算电影的推荐优先级
    * @param: topSimilarMoviesArray:与收到的评分电影(1个)最相似的电影列表
    * @param: recentRating：用户最近的几次评分
    * @param: mid2ScoreMap：收到的评分电影(1个)与所有电影的相似度的列表
    * @author:ggh
    * @updateTime: 2020/6/14 20:11
    **/
  def computeRecommendedLevel(topSimilarMoviesArray: Array[(Long, Double)],
                              recentRatings: ListBuffer[(Long, Double)],
                              mid2ScoreMap: mutable.HashMap[Long, Double],
                              collection: MongoCollection) = {

    //存放每一个待选电影和用户最近的几个评分电影的权重
    //如：待选电影有:11,22,33   最近的评分电影有:44,55,66
    // 11和最近评分电影的权重：(11,33.2) (11,23.3) (11,34.5)
    // 22和最近评分电影的权重：(22,33.2) (22,23.3) (22,34.5)
    // 33和最近评分电影的权重：(33,33.2) (33,23.3) (33,34.5)
    //以上9条记录存于scores中
    //    val scores: ListBuffer[(Long,Double)] = ListBuffer()
    val scores: ArrayBuffer[(Long, Double)] = ArrayBuffer[(Long, Double)]()

    //存放每一个待选电影的增强因子
    //如：(11,1) (11,1) (11,1) (22,1) (22,1) ...
    val increMap: mutable.HashMap[Long, Int] = mutable.HashMap[Long, Int]()

    //存放每一个待选电影的减弱因子
    //如：(11,1) (11,1) (11,1) (22,1) (22,1) ...
    val decreMap: mutable.HashMap[Long, Int] = mutable.HashMap[Long, Int]()

    for (topSimMovie <- topSimilarMoviesArray; recentRating <- recentRatings) {

      //获取待选电影和已评分电影的相似度
      val simScore: Double = getSimSore(topSimMovie._1, recentRating._1, collection)

      if (simScore >= 0) {
        scores += ((topSimMovie._1, simScore * recentRating._2))

        if (recentRating._2 > 60) {
          //如果最近评分电影分数大于60，增强因子加一
          increMap(topSimMovie._1) = increMap.getOrElse(topSimMovie._1, 0) + 1
        } else {
          //如果最近评分电影分数小于60，减弱因子加一
          decreMap(topSimMovie._1) = decreMap.getOrElse(topSimMovie._1, 0) + 1
        }

      }

    }


    scores.groupBy(_._1).map {
      case (mid, sims) => {
        (mid, sims.map(_._2).sum / sims.length + log(increMap.getOrElse(mid, 1)) + log(decreMap.getOrElse(mid, 1)))
      }
    }
      .toArray
      .sortWith(_._2 > _._2)

  }


  /**
    * @title:
    * @description: 获取与某个电影最相似的前num个电影
    * @param: num：要获取的最相似的前num电影
    * @param: mid2ScoreMap：某个电影的电影相似度列表
    * @author:ggh
    * @updateTime: 2020/6/14 17:31
    **/
  def getTopSimilarMovies(num: Int,
                          uid: Long,
                          mid2ScoreMap: mutable.HashMap[Long, Double],
                          connection: Connection,
                          recentRating: ListBuffer[(Long, Double)]
                         ) = {

    //从数据库获取该用户的评分记录

    var sqlResultBuffer: ListBuffer[(Long, Double, String)] = ListBuffer()
    var resultBuffer: ListBuffer[(Long, Double, String)] = ListBuffer()

    var sql = "select mid,rating,date from m_rating where uid=?"

    val preparedStatement: PreparedStatement = connection.prepareStatement(sql)
    preparedStatement.setLong(1, uid)
    val resultSet: ResultSet = preparedStatement.executeQuery()

    //遍历添加到list集合
    while (resultSet.next()) {
      val mid: Long = resultSet.getLong("mid")
      val rating: Double = resultSet.getDouble("rating")
      val date: String = resultSet.getString("date")
      sqlResultBuffer += ((mid, rating, date))
      //      mids += resultSet.getLong("mid")
    }

    //获取用户最近的几次评分
    val sortByDate: ListBuffer[(Long, Double, String)] = sqlResultBuffer.sortWith((x, y) => x._3.compareTo(y._3) > 0)
    sortByDate.remove(0)
    sortByDate.take(10).foreach(x => recentRating += ((x._1, x._2)))

    //获取电影id
    val mids: ListBuffer[Long] = sqlResultBuffer.map(_._1)

    //利用用户的评分记录从mid2ScoreMap过滤掉已评过分的记录
    val filteredArray: Array[(Long, Double)] = mid2ScoreMap.toArray.filter(x => !mids.contains(x._1))

    //将过滤后的数据按相似度排序,取前num个
    filteredArray.sortWith(_._2 > _._2).take(num)

  }


  //获取数据库连接
  def getConnection(): Connection = {

    Class.forName(Constant.DRIVER_DB)

    val connection: Connection = DriverManager.getConnection(Constant.URL_DB, Constant.USER_DB, Constant.PASSWORD_DB)

    connection
  }

  //取10的对数
  def log(m: Int): Double = {
    math.log(m) / math.log(10)
  }

}
