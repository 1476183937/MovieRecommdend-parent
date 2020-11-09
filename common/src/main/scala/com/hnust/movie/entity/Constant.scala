package com.hnust.movie.entity

/**
  * @Title:
  * @Author: ggh
  * @Date: 2020/6/2 19:27
  */
object Constant {

  //数据库中电影详情表的名称
  val MOVIE_INFO_DB = "m_movie_info"
  //数据库中评分表的名称
  val MOVIE_RATING_DB = "m_rating"
  //数据库中用户信息表的名称
  val USERINFO_DB = "m_userinfo"
  //数据库中电影评论表的名称
  val COMMENT_DB = "m_comment"
  //数据库中用户浏览历史表的名称
  val SCAN_HISTORY_DB = "m_scan_history"
  //数据库中用户收藏表的名称
  val USER_COLLECTION_DB = "m_user_collection"


  //数据库用户名
  val USER_DB="root"
  //数据库密码
  val PASSWORD_DB = "root"

  //数据库名
  val DB_NAME = "movie"

  //连接数据库的url
  val URL_DB = "jdbc:mysql://localhost:3306/movie?useUnicode=true&characterEncoding=UTF-8&useSSL=false&serverTimezone=GMT%2B8"

  //连接数据库的驱动
  val DRIVER_DB = "com.mysql.jdbc.Driver"

  //连接mongodb的url
  val MONGO_URI = "mongodb://localhost:27017/recommender"

  //mongo的数据库名称
  val MONGODB_NAME = "recommender"

  //在mongoDB中存放相似电影推荐数据的collection名称
  val SIMILAR_MOVIE_RECOMMENDATION_MONGODB = "similarMovieRecommendation"

  //在mongoDB中存放动漫排行榜的数据的collection的名称
  val TOP_COMICS_MONGODB = "topComics"

  //在mongoDB中存放电影排行榜的数据的collection的名称
  val TOP_MOVIES_MONGODB = "topMovies"

  //在mongoDB中存放本周热榜的数据的collection的名称
  val TOP_MOVIES_MONGTH_MONGODB = "topMovieOfMonth"

  //在mongoDB中存放本月热榜的数据的collection的名称
  val TOP_MOVIES_WEEK_MONGODB = "topMovieOfWeek"

  //在mongoDB中存放各个类别的热度排名前10的电影的数据的collection的名称
  val TOP_MOVIES_OFCATEGORY_MONGODB = "topMoviesOfCategory"

  //在mongoDB中存放对每个用户的推荐电影数据的collection的名称
  val USER_RECOMMENDATION_MONGODB = "userRecommendation"

  //在mongoDB中存放综合排行榜的数据的collection的名称，包含热播榜、北美榜、大陆榜、好评榜
  val  MULTIPLE_RAKING_MONGODB = "multipleRanking"

  //在mongoDB中存放电影相似度矩阵的collection的名称
  val MID_2_SIMILAR_SCORE = "mid2SimilarScore"

  //在mongodb中存放session统计结果的collection名称
  val SESSION_ANALYSE_MONGODB = "sessionAnalyse"
  val SESSION_TOP10_CATEGORIES = "sessionTop10Categories"

  //在mongodb中存放session统计中各类别的活跃session
  val SESSION_TOP_SESSION_OF_CATEGORY_MONGODB = "topSessionOfCategory"

  //在mongodb中存放top10类别中的活跃session的详情数据
  val SESSION_TOP_SESSION_DETAIL_OF_CATEGORY_MONGODB = "topSessionDetailOfCategory"

  //在mongodb中存放页面转化率的collection名称
  val PAGE_CONVERT_RATE_MONGODB = "pageConvertRate"


  //在mongodb中存放各个区域top电影信息的collection名称
  val TOP_MOVIES_OF_AREA = "TopMoviesOfArea"

}
