package com.hnust.movie

import java.io.FileInputStream
import java.util.Properties

import com.hnust.movie.entity.CaseClasses.{CityInfo, UserAction}
import com.hnust.movie.entity.Constant
import com.hnust.movie.utils.DateUtil
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}


case class TopMoviesOfArea(date:String, cityId:Int, cityName:String,movieId:Long, movieName:String, count:Long)

/**
  * @Title:统计各个城市的top10电影
  * 来源数据格式：用户行为数据：动作日期|用户id|sessionId|页面id|查询词|点击类别id|点击电影id|收藏电影id|评分电影id|评论电影id|城市id
  * 全国城市信息表：id|name|pid|sname|level|citycode|yzcode|mername|Lng|Lat|pinyin
  *             （主键id，地域名称，上级id，地域简称，地域级别，区域编码，邮政编码，组合名称，经度，纬度，区域名称的拼音）
  * @Author: ggh
  * @Date: 2020/7/9 20:09
  */
object TopMoviesOfArea {

  def main(args: Array[String]): Unit = {

    val sparkConf: SparkConf = new SparkConf().setAppName("TopMoviesOfArea").setMaster("local[*]")

    val spark: SparkSession = SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate()

    val sc: SparkContext = spark.sparkContext

    //加载配置文件
    val path: String = getClass.getResource("/application.properties").getPath
    val prop: Properties = new Properties()

    prop.load(new FileInputStream(path))

    //获取指定日期内的数据: (城市id,电影id)
    val cityId2MovieIdRDD: RDD[(Long, Long)] = getDataByRangeDate(spark, prop)

    import spark.implicits._

    //统计各个城市中各个电影的点击数量
    //将rdd转换成DF
    val cityId2MovieIdDF: DataFrame = cityId2MovieIdRDD.toDF("cityId,movieId")
    //建临时表
    cityId2MovieIdDF.createOrReplaceTempView("tmp_cityId_movieId")
    //sql查询
    val cityId2CountDF: DataFrame = spark.sql(
      """
        |select
        |cityId,
        |movieId,
        |count(movieId) count
        |from tmp_cityId_movieId
        |group by cityId,movieId
      """.stripMargin)

    //建临时表
    cityId2CountDF.createOrReplaceTempView("tmp_cityId_movieId_count")

    //获取各城市的top电影
    //从配置文件获取各个城市要统计top电影个数
    val topMoviesCount: String = prop.getProperty("top.movies.count")

    //(cityId,movieId,count,rank)
    val topMoviesOfArea: DataFrame = spark.sql("select cityId,movieId,count,row_number() OVER(PARTITION BY cityId ORDER BY count) rank " +
      "from tmp_cityId_movieId_count where rank <= " + topMoviesCount)

    //获取城市的信息:(城市id，城市信息)
    val cityId2CityInfoRDD: RDD[(Int, CityInfo)] = getAllCityInfo(spark).map(item => (item.id, item))

    import spark.implicits._

    //将 topMoviesOfArea 和 cityInfoRDD 进行join,得到含有城市名称的信息
    //topMoviesWithCityInfoRDD ：(电影id,(城市id，数量，区域名称))
    val topMoviesWithCityInfoRDD: RDD[(Long, (Int, Long, String))] = topMoviesOfArea.rdd.map {
      row => {
        (row.getAs[Int]("cityId").toInt, row) //转换成二元组
      }
    }
      .join(cityId2CityInfoRDD)
      .map {
        case (cid, (row, cityInfo)) => {
          (row.getAs[Long]("movieId"), (cid,row.getAs[Long]("count"), cityInfo.name))
        }
      }

    //提取出电影id
    val midsArray: Array[Long] = topMoviesWithCityInfoRDD.map(_._1).collect()

    //获取电影信息：(电影id，电影名称)
    val mid2MnameRDD: RDD[(Long, String)] = getMovieInfo(spark, midsArray)

    //获取当前时间
    val date: String = DateUtil.getCurrentTime("yyyy-MM-dd HH:mm:ss")

    //将 topMoviesWithCityInfoRDD 和 mid2MnameRDD 进行join，得到含有电影名称的信息
    val TopMoviesOfAreaDF: DataFrame = topMoviesWithCityInfoRDD.join(mid2MnameRDD)
      .map {
        case (mid, ((cid, count, cname), mname)) => {
          //封装成样例类
          TopMoviesOfArea(date, cid, cname, mid, mname, count)
        }
      }.toDF()

    //保存到mongodb
    TopMoviesOfAreaDF.write
      .format("com.mongodb.spark.sql")
      .option("uri",Constant.MONGO_URI)
      .option("collection",Constant.TOP_MOVIES_OF_AREA)
      .mode(SaveMode.Append)
      .save()

    spark.stop()


  }


  /**
  *@title:
  *@description: 获取指定日期内的数据
  *@param: spark
  *@param: prop
  *@author:ggh
  *@updateTime: 2020/7/9 20:51
  **/
  def getDataByRangeDate(spark: SparkSession, prop: Properties) ={

    //从配置文件获取过滤的起始和结束日期
    val startDate: String = prop.getProperty("filter.start.date")
    val endDate: String = prop.getProperty("filter.end.date")

    import spark.implicits._

    //将其转换成时间戳的形式
    val startTime: Long = DateUtil.dateStr2TimeMillis(startDate + " 00:00:00")
    val endTime: Long = DateUtil.dateStr2TimeMillis(endDate + " 00:00:00")

    //从hive读取数据，转换成UserAction格式的rdd
    val userActionsRDD: RDD[UserAction] = spark.sql("select * from user_visit_action where date >= " + startTime + " and date <= " + endTime)
      .as[UserAction]
      .rdd

    //从用户行为数据中提取出电影id，点击电影id、收藏电影id、评论电影id等都只算一个电影id
    userActionsRDD
        .filter{ //把没有电影id的数据过滤掉
          action => {
            var ok = false

            if (action.clickMovieId > 0 || action.collectMovieId >0 || action.ratingMovieId > 0 || action.commentMovieId > 0)
            ok = true

            ok
          }
        }
      .map{
          action => {

            var movieId:Long = 0L
            if (action.clickMovieId != null && action.clickMovieId > 0) movieId = action.clickMovieId
            else if (action.collectMovieId != null && action.collectMovieId >0) movieId = action.collectMovieId
            else if (action.ratingMovieId != null && action.ratingMovieId > 0) movieId = action.ratingMovieId
            else movieId = action.commentMovieId

            //转换成：("用户id，电影id,城市id")的一元组
            (action.userId+"," + movieId+","+action.cityId)
          }
        }
        .distinct() //去重
        .map{
          str => {
            val splits: Array[String] = str.split(",")
            (splits(2).toLong, splits(1).toLong)
          }
        }

//    userActionsRDD

  }

  /**
  *@title:
  *@description: 获取全国所有的城市信息
  *@param: spark
  *@author:ggh
  *@updateTime: 2020/7/9 20:52
  **/
  def getAllCityInfo(spark: SparkSession) ={

    import spark.implicits._
    spark.sql("select * from region")
      .as[CityInfo]
      .rdd

  }


  /**
  *@title:
  *@description: 获取电影信息
  *@param: spark
  *@param: midsArray：电影id数组
  *@author:ggh
  *@updateTime: 2020/7/9 21:51
  **/
  def getMovieInfo(spark: SparkSession, midsArray: Array[Long]) ={

    val buffer: StringBuffer = new StringBuffer()
    //将所有电影id拼接成字符串
    midsArray.foreach(mid => buffer.append(mid+","))

    //将最后一个逗号去掉
    val queryMids: String = buffer.toString.substring(0,buffer.toString.length-1)

    //从hive表中获取数据
    spark.sql("select mid,movie_name from movie_info where mid in (" + queryMids + ")")
      .rdd
      .map(item => (item.getAs[Long]("mid"), item.getAs[String]("movie_name")))

  }

}
