package com.hnust.movie

import java.util.concurrent.ConcurrentHashMap
import java.util.{Date, Properties}

import com.hnust.movie.entity.CaseClasses.{CategoryStatistics, Comment_DB, MovieInfo_DB, Movie_MongoDB, MultipleRanking, Rating_DB, ScanHistory_DB, TOP_COMICS_MONGODB, TOP_MOVIES_MONGODB, TopMoviesOfMonth, TopMoviesOfWeek, UserCollection_DB}
import com.hnust.movie.entity._
import com.hnust.movie.utils.DateUtil
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SaveMode, SparkSession}

/**
  * @Title:离线统计模块
  * @Author: ggh
  * @Date: 2020/6/2 19:17
  */
object StatisticsRecommender {

  def main(args: Array[String]): Unit = {

    //创建sparkConf
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]")
      .setAppName("StatisticsRecommender")
      .set("spark.default.parallelism","10")
      .set("spark.sql.shuffle.partitions","20")

    //创建sparkSession
    val spark: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()
    val sparkContext: SparkContext = spark.sparkContext

    import spark.implicits._

    //读取数据库的电影详情数据
    var movieInfoDF: DataFrame = spark.read.format("jdbc")
      .option("driver", "com.mysql.jdbc.Driver")
      .option("url", Constant.URL_DB)
      .option("dbtable", Constant.MOVIE_INFO_DB)
      .option("user", Constant.USER_DB)
      .option("password", Constant.PASSWORD_DB)
      .load()
      .as[MovieInfo_DB]
      .toDF()
      .cache()

    movieInfoDF.createOrReplaceTempView("movieInfoDF")

    //读取数据库的评分数据
    val ratingDF: DataFrame = spark.read.format("jdbc")
      .option("driver", "com.mysql.jdbc.Driver")
      .option("url", Constant.URL_DB)
      .option("dbtable", Constant.MOVIE_RATING_DB)
      .option("user", Constant.USER_DB)
      .option("password", Constant.PASSWORD_DB)
      .load()
      .as[Rating_DB]
      .toDF()
      .cache()

    //读取电影评论表
    val commentDF: DataFrame = spark.read.format("jdbc")
      .option("driver", "com.mysql.jdbc.Driver")
      .option("url", Constant.URL_DB)
      .option("dbtable", Constant.COMMENT_DB)
      .option("user", Constant.USER_DB)
      .option("password", Constant.PASSWORD_DB)
      .load()
      .as[Comment_DB]
      .toDF()
      .cache()


    //读取用户浏览历史表的数据
    val scanHistoryDF: DataFrame = spark.read.format("jdbc")
      .option("driver", "com.mysql.jdbc.Driver")
      .option("url", Constant.URL_DB)
      .option("dbtable", Constant.SCAN_HISTORY_DB)
      .option("user", Constant.USER_DB)
      .option("password", Constant.PASSWORD_DB)
      .load()
      .as[ScanHistory_DB]
      .toDF()
      .cache()

    //读取用户收藏表的数据
    val userCollectionDF: DataFrame = spark.read.format("jdbc")
      .option("driver", "com.mysql.jdbc.Driver")
      .option("url", Constant.URL_DB)
      .option("dbtable", Constant.USER_COLLECTION_DB)
      .option("user", Constant.USER_DB)
      .option("password", Constant.PASSWORD_DB)
      .load()
      .as[UserCollection_DB]
      .toDF()
      .cache()

    //建临时表
    //    movieInfoDF.createOrReplaceTempView("movieInfo")
    ratingDF.createOrReplaceTempView("movieRating")
    commentDF.createOrReplaceTempView("movieComment")
    scanHistoryDF.createOrReplaceTempView("scanHistory")
    userCollectionDF.createOrReplaceTempView("userCollection")


    movieInfoDF = movieInfoDF.filter(info => info.getAs("categories")!=null)

    //    从电影详情表获取：(mid, movie_name,categories,release_date)
    val movieInfoDF2: DataFrame = movieInfoDF.map(info => {
      (info.getAs[Long]("mid"),
        info.getAs[String]("movie_name"),
        info.getAs[String]("categories").replace("|", ","),
        info.getAs[String]("release_date"),
        info.getAs[String]("location")
      )
    }).toDF("mid", "movie_name", "categories", "release_date","location")

    movieInfoDF2.createOrReplaceTempView("movieInfoDF2")

    //从评分表里面获取每个电影的评分数：(mid,count)
    val ratingMid2Count: DataFrame = spark.sql("select mid, count(*) count from movieRating group by mid")
    ratingMid2Count.createOrReplaceTempView("ratingMid2Count")

    //从评论表中里面获取每个电影的评论数：(mid,count)
    val commentMid2Count: DataFrame = spark.sql("select mid, count(*) count from movieComment group by mid")
    commentMid2Count.createOrReplaceTempView("commentMid2Count")

    //从收藏表中获取每个电影的收藏数：(mid,count)
    val collectionMid2Count: DataFrame = spark.sql("select mid, count(*) count from userCollection group by mid")
    collectionMid2Count.createOrReplaceTempView("collectionMid2Count")

    //从浏览历史表中获取每个电影的观看数：(mid,count)
    val playMid2Count: DataFrame = spark.sql("select mid, count(*) count from scanHistory group by mid")
    playMid2Count.createOrReplaceTempView("playMid2Count")

    //join得到：(mid，movie_name，categories，release_date,rating_count,comment_count,collection_count,play_count)
    val movieAggrDf: DataFrame = spark.sql(
      """
        select m2.*,
        if(r.count is null,0,r.count) rating_count,
        if(c.count is null, 0,c.count) comment_count,
        if(co.count is null, 0,co.count) collection_count,
        if(p.count is null, 0, p.count) play_count
        from movieInfoDF2 m2 left join ratingMid2Count r on m2.mid=r.mid
        left join commentMid2Count c on m2.mid=c.mid
        left join collectionMid2Count co on m2.mid=co.mid
        left join playMid2Count p on m2.mid=p.mid
      """.stripMargin)
        .toDF("mid","movie_name","categories","release_date","location","rating_count","comment_count","collection_count","play_count")

    //计算每个电影的热度值
    val movieWithHotdegree:RDD[(Long, String, String, Double,String)]= calculateHotDegreeByDF(movieAggrDf)
    movieWithHotdegree.toDF("mid","movie_name","categories","hot_degree","location").createOrReplaceTempView("movieAggrInfo")

//    movieWithHotdegree.toDF().show()

    //TODO:统计每个类别中热度值前十的电影数据
    //进行列转行,将类别炸裂开来
    val mid2Category: DataFrame = spark.sql("select mid,movie_name,hot_degree,category from movieAggrInfo lateral view explode(split(categories,',')) tmp as category")

    //将电影详情数据转换成map形式，以便后面使用
    val movieInfoMap: collection.Map[Long, Row] = movieInfoDF.rdd.map(info => {
      (info.getAs[Long]("mid"), info)
    }).collectAsMap()

    //根据类别进行分组并统计
    val categoryStatistic: RDD[CategoryStatistics] = mid2Category.rdd.groupBy(row => row.getAs[String]("category")).map {
      case (category, movieList) => {
        (category, movieList.toList.sortWith(_.getAs[Double]("hot_degree") > _.getAs[Double]("hot_degree")).take(10))
      }
    }.map{
      case (category, movieList) => {
        CategoryStatistics(category,DateUtil.getCurrentTime("yyyy-MM-dd"),
          movieList.map(info => {
            var mid = info.getAs[Long]("mid")
            Movie_MongoDB(mid,
              info.getAs[String]("movie_name"),
              info.getAs[Double]("hot_degree"),
              movieInfoMap.get(mid).get.getAs[String]("img_urls"),
              movieInfoMap.get(mid).get.getAs[Double]("rating_num"),
              movieInfoMap.get(mid).get.getAs[String]("categories")
            )
          })
        )
      }
    }
    

    //将各类别的统计信息保存到Mongodb
    categoryStatistic.toDF().write
      .option("uri",Constant.MONGO_URI)
      .option("collection",Constant.TOP_MOVIES_OFCATEGORY_MONGODB)
      .mode(SaveMode.Append)
      .format("com.mongodb.spark.sql")
      .save()


    //TODO:计算电影排行榜

    //过滤出电影的数据
    val filterMovie = movieWithHotdegree.filter( !_._3.contains("动画")).toDF("mid","movie_name","categories","hot_degree","location")

    filterMovie.rdd

    val movieInfoList: List[Movie_MongoDB] = filterMovie.rdd.sortBy(_.getAs[Double]("hot_degree"),false).take(10).map(info => {
      val mid = info.getAs[Long]("mid")

      Movie_MongoDB(mid, info.getAs[String]("movie_name"),
        info.getAs[Double]("hot_degree"),
        movieInfoMap.get(mid).get.getAs[String]("img_urls"),
        movieInfoMap.get(mid).get.getAs[Double]("rating_num"),
        movieInfoMap.get(mid).get.getAs[String]("categories")
      )

    }).toList


    sparkContext.makeRDD(List(TOP_MOVIES_MONGODB(DateUtil.getCurrentTime("yyyy-MM-dd"),movieInfoList))).toDF().write
      .option("uri",Constant.MONGO_URI)
      .option("collection",Constant.TOP_MOVIES_MONGODB)
      .mode(SaveMode.Append)
      .format("com.mongodb.spark.sql")
      .save()


    //TODO:动漫排行榜
    val filterComic = movieWithHotdegree.filter( _._3.contains("动画")).toDF("mid","movie_name","categories","hot_degree","location")

    filterComic.show(truncate = false)

    val comicList: List[Movie_MongoDB] = filterComic.rdd.sortBy(_.getAs[Double]("hot_degree"),false).take(10).map(info => {
      val mid = info.getAs[Long]("mid")

      Movie_MongoDB(mid, info.getAs[String]("movie_name"),
        info.getAs[Double]("hot_degree"),
        movieInfoMap.get(mid).get.getAs[String]("img_urls"),
        movieInfoMap.get(mid).get.getAs[Double]("rating_num"),
        movieInfoMap.get(mid).get.getAs[String]("categories")
      )

    }).toList


    sparkContext.makeRDD(List(TOP_COMICS_MONGODB(DateUtil.getCurrentTime("yyyy-MM-dd"),comicList))).toDF().write
      .option("uri",Constant.MONGO_URI)
      .option("collection",Constant.TOP_COMICS_MONGODB)
      .mode(SaveMode.Append)
      .format("com.mongodb.spark.sql")
      .save()

    //TODO:热播榜multipleRanking
    val multipleRankingList: Array[(Long, String, String, Double,String)] = movieWithHotdegree.sortBy(_._4,false).take(10)
    //转换成Movie_MongoDB
    val multipleRankingArray: Array[Movie_MongoDB] = multipleRankingList.map(row => {
      Movie_MongoDB(row._1, row._2, row._4,
        movieInfoMap.get(row._1).get.getAs[String]("img_urls"),
        movieInfoMap.get(row._1).get.getAs[Double]("rating_num"),
        movieInfoMap.get(row._1).get.getAs[String]("categories")
      )
    })

    //写入mongo
    val date: String = DateUtil.getCurrentTime("yyyy-MM-dd")
    sparkContext.makeRDD(List(MultipleRanking(date,"hot_ranking",multipleRankingArray))).toDF("date","category","movieList").write
        .option("uri",Constant.MONGO_URI)
        .option("collection",Constant.MULTIPLE_RAKING_MONGODB)
        .mode(SaveMode.Append)
        .format("com.mongodb.spark.sql")
        .save()


    //TODO:北美榜
    //过滤出发布地区为美国、法国、加拿大、西班牙、意大利等的电影数据
    val filterLocationMovies: RDD[(Long, String, String, Double, String)] = movieWithHotdegree.filter(info => {
      info._5.contains("美国") || info._5.contains("法国") || info._5.contains("加拿大") || info._5.contains("西班牙") ||
        info._5.contains("西班牙") || info._5.contains("意大利")
    })

    //排序，取前十个，并转换成Movie_MongoDB
    val locationMoviesArray: Array[Movie_MongoDB] = filterLocationMovies.sortBy(_._4, false).take(10).map(info => {
      Movie_MongoDB(info._1, info._2, info._4,
        movieInfoMap.get(info._1).get.getAs[String]("img_urls"),
        movieInfoMap.get(info._1).get.getAs[Double]("rating_num"),
        movieInfoMap.get(info._1).get.getAs[String]("categories")
      )
    })

    sparkContext.makeRDD(List(MultipleRanking(date,"north_america",locationMoviesArray))).toDF("date","category","movieList").write
        .option("uri",Constant.MONGO_URI)
        .option("collection",Constant.MULTIPLE_RAKING_MONGODB)
        .mode(SaveMode.Append)
        .format("com.mongodb.spark.sql")
        .save()

    //TODO:好评榜,直接从原始的电影数据中取，按评分和发布日期排序
    val goodRankingArray: Array[Movie_MongoDB] = movieInfoDF.rdd
                                                  .sortBy(_.getAs[String]("release_date"),false)
                                                  .sortBy(_.getAs[Double]("rating_num"),false)
                                                  .take(10)
                                                  .map(info => {
      Movie_MongoDB(info.getAs[Long]("mid"), info.getAs[String]("movie_name"),
        0.0, info.getAs[String]("img_urls"), info.getAs[Double]("rating_num"),
        info.getAs[String]("categories"))
    })

    sparkContext.makeRDD(List(MultipleRanking(date,"good_ranking",goodRankingArray))).toDF("date","category","movieList").write
      .option("uri",Constant.MONGO_URI)
      .option("collection",Constant.MULTIPLE_RAKING_MONGODB)
      .mode(SaveMode.Append)
      .format("com.mongodb.spark.sql")
      .save()

    //TODO:大陆榜
    val mainlandRankingArray: Array[Movie_MongoDB] = movieWithHotdegree.filter(_._5.contains("大陆")).sortBy(_._4, false).take(10).map(info => {
      Movie_MongoDB(info._1, info._2, info._4,
        movieInfoMap.get(info._1).get.getAs[String]("img_urls"),
        movieInfoMap.get(info._1).get.getAs[Double]("rating_num"),
        movieInfoMap.get(info._1).get.getAs[String]("categories")
      )
    })

    sparkContext.makeRDD(List(MultipleRanking(date,"mainland_ranking",mainlandRankingArray))).toDF("date","category","movieList").write
      .option("uri",Constant.MONGO_URI)
      .option("collection",Constant.MULTIPLE_RAKING_MONGODB)
      .mode(SaveMode.Append)
      .format("com.mongodb.spark.sql")
      .save()



    //TODO:本月排行榜
    //获取当前时间的 年-月
    val currentMonth: String = DateUtil.getCurrentTime("yyyy-MM")

    var sql = "SELECT m.mid,m.movie_name,0.0 hot_degree,m.img_urls,m.rating_num,m.categories FROM movieInfoDF m JOIN (select mid, count(*) count from movieRating where date like '"+currentMonth+"%' group by mid ORDER BY count desc) t1 ON m.mid=t1.mid"
    val monthRankingArray: Array[Movie_MongoDB] = spark.sql(sql).toDF("mid", "movie_name", "hot_degree", "img_urls", "rating_num", "categories").rdd.take(10).map(info => {
      Movie_MongoDB(info.getAs[Long]("mid"),
        info.getAs[String]("movie_name"), 0.0,
        info.getAs[String]("img_urls"),
        info.getAs[Double]("rating_num"),
        info.getAs[String]("categories")
      )
    })

    sparkContext.makeRDD(List(TopMoviesOfMonth(date,monthRankingArray))).toDF("date","movieList").write
      .option("uri",Constant.MONGO_URI)
      .option("collection",Constant.TOP_MOVIES_MONGTH_MONGODB)
      .mode(SaveMode.Append)
      .format("com.mongodb.spark.sql")
      .save()


    //TODO:本周排行榜

    val mondayDate: String = DateUtil.getMondayByDate(new Date())
    val sundayDate: String = DateUtil.getSundayByDate(new Date())

    sql = "SELECT m.mid, m.movie_name, 0.0 hot_degree, m.img_urls, m.rating_num, m.categories FROM movieInfoDF m JOIN (SELECT  mid, count(*) count FROM movieRating where date>'"+mondayDate+"' AND date<'"+sundayDate+" 23:59:59' group by mid ORDER BY count DESC) t1 ON m.mid=t1.mid"

    val weekRankingArray: Array[Movie_MongoDB] = spark.sql(sql)
      .toDF("mid", "movie_name", "hot_degree", "img_urls", "rating_num", "categories").rdd.take(10).map(info => {
      Movie_MongoDB(info.getAs[Long]("mid"),
        info.getAs[String]("movie_name"), 0.0,
        info.getAs[String]("img_urls"),
        info.getAs[Double]("rating_num"),
        info.getAs[String]("categories")
      )
    })

    sparkContext.makeRDD(List(TopMoviesOfWeek(date,weekRankingArray))).toDF("date","movieList").write
      .option("uri",Constant.MONGO_URI)
      .option("collection",Constant.TOP_MOVIES_WEEK_MONGODB)
      .mode(SaveMode.Append)
      .format("com.mongodb.spark.sql")
      .save()

    spark.stop()

  }


  /*
  *@title:
  *@description: 根据电影聚合信息计算出每个电影的热度值
  *@param: movieAggrDF:(mid，movie_name，categories，release_date,rating_count,comment_count,collection_count,play_count)
  *@author:ggh
  *@updateTime: 2020/6/3 11:01
  **/
  def calculateHotDegreeByDF(movieAggrDF:DataFrame):RDD[(Long, String, String, Double, String)]={

    var movieAggrDF1 = movieAggrDF.filter(info => info.getAs[String]("release_date").length >=10 && !info.getAs[String]("release_date").substring(0, 10).contains("("))

    val movieWithHotdegree: RDD[(Long, String, String, Double, String)] = movieAggrDF1.rdd.map(info => {

      val release_date: String = info.getAs[String]("release_date").substring(0, 10)

      val time: Int = DateUtil.timeDiffHoursToNow(release_date)

      //计算出热度值
      val hotDegree: Double = calculateHotDegree(info.getAs[Long]("rating_count").toInt,
        info.getAs[Long]("comment_count").toInt,
        info.getAs[Long]("collection_count").toInt,
        info.getAs[Long]("play_count").toInt,
        time.toInt)

      (info.getAs[Long]("mid"), info.getAs[String]("movie_name"), info.getAs[String]("categories"), hotDegree,info.getAs[String]("location"))

    })
    movieWithHotdegree

  }

  //计算电影的热度值
  /** aaa 80 2000 300 1500 15
    *
    * @title: Score = (P-1) *0.5+C*0.1+L*0.2+B*0.2/ (T+2)^G,  G的默认值为1.8
    * @description:
    * @param: ratingNum:评分数( -1 是去掉文章提交人的票),即参数P
    * @param: commentNum:评论数，即参数C
    * @param: collectionNum:收藏数，即参数L
    * @param: playNum:播放量，即参数B
    * @param: time:从文章提交至今的时间(小时)，即参数T
    * @author:ggh
    * @updateTime: 2020/6/2 20:23
    * */
  def calculateHotDegree(ratingNum: Int,
                commentNum: Int,
                collectionNum: Int,
                playNum: Int,
                time: Int): Double = {

    var score: Double = 0.0
    val G = 1.8

    score = (ratingNum - 1) * 0.5 + commentNum * 0.1 + collectionNum * 0.2 + playNum / Math.pow((time + 2), G)

    score

  }


}
