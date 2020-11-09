package com.hnust.movie.entity

/**
  * @Title:
  * @Author: ggh
  * @Date: 2020/6/2 19:17
  */
object CaseClasses {

  //mongo配置类
  case class MongoConfig(uri: String, db: String)

  //评分样例类
  case class Rating(uid: String, mid: String, rating: String, date: String)

  //数据库中的评分表样例类
  case class Rating_DB(rating_id: Long, uid: Long, mid: Long, rating: Double, date: String)

  //数据库中电影详情表的样例类
  case class MovieInfo_DB(mid: Long,
                          movie_name: String,
                          img_urls: String,
                          directors: String,
                          screen_writer: String,
                          main_actors: String,
                          categories: String,
                          location: String,
                          language: String,
                          release_date: String,
                          run_time: String,
                          alias: String,
                          summery: String,
                          rating_num: Double,
                          off_shelf: Int,
                          hot_degree: Int,
                          view_num: Long,
                          play_url: String)

  //数据库中电影评论表的样例类
  case class Comment_DB(comment_id: String,
                        uid: Long, mid: Long,
                        content: String,
                        date: String,
                        deleted: Int,
                        like_num: Int,
                        dislike_num: Int)


  //数据库中用户浏览历史的样例类
  case class ScanHistory_DB(scan_id: String,
                            uid: Long,
                            mid: Long,
                            movie_name: String,
                            image: String,
                            date: String,
                            deleted: Int)

  //数据库中用户收藏表的名称
  case class UserCollection_DB(collection_id: String,
                               mid: Long,
                               uid: Long,
                               image: String,
                               movie_name: String,
                               date: String,
                               deleted: Int)

  //电影统计信息在mongodb中的存储格式
  //  case class Movie_MongoDB(mid:Long,movie_name:String,hot_degree:Double,imgUrl:String,ratingNum:Double,categories:String)
  case class Movie_MongoDB(mid: Long, movieName: String, hotDegree: Double, imgUrls: String, ratingNum: Double, categories: String)

  //在mongodb中存储类别统计信息格式
//  case class CategoryStatistics(category: String, time: String, movieList: Seq[Movie_MongoDB])
  case class CategoryStatistics(category: String, date: String, movieList: Seq[Movie_MongoDB])

  //在mongodb中存储电影排行榜前十的统计信息格式
  case class TOP_MOVIES_MONGODB(date: String, movieList: Seq[Movie_MongoDB])

  //在mongodb中存储动漫排行榜前十的统计信息格式
  case class TOP_COMICS_MONGODB(date: String, movieList: Seq[Movie_MongoDB])

  //综合排行榜的样例类，包含热播榜，北美榜、大陆榜、好评榜，用category来区别这四种
  case class MultipleRanking(date: String, category: String, movieList: Seq[Movie_MongoDB])

  //在mongodb中存储本月排行榜前十的统计信息格式
  case class TopMoviesOfMonth(date: String, movieList: Seq[Movie_MongoDB])

  //在mongodb中存储本周排行榜前十的统计信息格式
  case class TopMoviesOfWeek(date: String, movieList: Seq[Movie_MongoDB])

  //电影推荐的存储格式
  case class MovieResc(mid: Long,
                       movieName: String,
                       hotDegree: Double,
                       imgUrls: String,
                       ratingNum: Double,
                       categories: String,
                       score: Double)

  //在mongodb中存储每个电影下与其相似的电影的数据格式
  case class SimilarMovieRecommendation(date: String, mid: Long, simliarMovies: Seq[MovieResc])

  //
  case class Mid2Score(mid: Long, score: Double)

  //存放电影相似度矩阵：mid2Score表示每一个电影和电影id为mid的电影的相似度
  case class Mid2SimilarScore(date: String, mid: Long, mid2Score: Seq[Mid2Score])

  //  case class Mid2SimilarScoreList(date:String,mid:Long,mid2Score:List[Mid2Score])

  //mongodb中用户推荐电影collection的存储格式
  case class UserRecommendation(date: String, uid: Long, userResc: Seq[MovieResc])

  //数据库中电影的详情表的存储格式
  case class MoveiInfo_DB(
                           mid: Long,
                           movieName: String,
                           imgUrls: String,
                           directors: String,
                           screenWriter: String,
                           mainActors: String,
                           categories: String,
                           location: String,
                           language: String,
                           releaseDate: String,
                           runTime: String,
                           alias: String,
                           summery: String,
                           ratingNum: Float,
                           offShelf: Integer,
                           hotDegree: Integer,
                           viewNum: Long,
                           playUrl: String,
                           extend2: String
                         )

  //登录时间样例类：eventType表示登录成功(success)或失败(fail)
  case class LoginEvent(uid: Long, ip: String, eventType: String, timestamp: Long)


  /**
    * session会话分析的聚合统计表
    *
    * @param taskid                     当前计算批次的 ID
    * @param session_count              所有 Session 的总和
    * @param visit_length_1s_3s_ratio   1-3sSession 访问时长占比
    * @param visit_length_4s_6s_ratio   4-6sSession 访问时长占比
    * @param visit_length_7s_9s_ratio   7-9sSession 访问时长占比
    * @param visit_length_10s_30s_ratio 10-30sSession 访问时长占比
    * @param visit_length_30s_60s_ratio 30-60sSession 访问时长占比
    * @param visit_length_1m_3m_ratio   1-3mSession 访问时长占比
    * @param visit_length_3m_10m_ratio  3-10mSession 访问时长占比
    * @param visit_length_10m_30m_ratio 10-30mSession 访问时长占比
    * @param visit_length_30m_ratio     30mSession 访问时长占比
    * @param step_length_1_3_ratio      1-3 步长占比
    * @param step_length_4_6_ratio      4-6 步长占比
    * @param step_length_7_9_ratio      7-9 步长占比
    * @param step_length_10_30_ratio    10-30 步长占比
    * @param step_length_30_60_ratio    30-60 步长占比
    * @param step_length_60_ratio       大于 60 步长占比
    */
  case class SessionAggrStat(taskid: String,
                             session_count: Long,
                             visit_length_1s_3s_ratio: Double,
                             visit_length_4s_6s_ratio: Double,
                             visit_length_7s_9s_ratio: Double,
                             visit_length_10s_30s_ratio: Double,
                             visit_length_30s_60s_ratio: Double,
                             visit_length_1m_3m_ratio: Double,
                             visit_length_3m_10m_ratio: Double,
                             visit_length_10m_30m_ratio: Double,
                             visit_length_30m_ratio: Double,
                             step_length_1_3_ratio: Double,
                             step_length_4_6_ratio: Double,
                             step_length_7_9_ratio: Double,
                             step_length_10_30_ratio: Double,
                             step_length_30_60_ratio: Double,
                             step_length_60_ratio: Double
                            )


  //用户行为样例类
  case class UserAction(
                         date: Long, //日期，时间戳
                         userId: Long, //用户id
                         sessionId: String, //session的id
                         pageId: String, //访问的页面url
                         searchKeyword: String, //搜索关键词
                         clickCategoryId: Long, //点击的类别id
                         collectCategoryId: Long, //收藏的类别id
                         ratingCategoryId: Long, //评分的类别id
                         commentCategoryId: Long, //评论的类别id
                         clickMovieId: Long, //点击的电影id
                         collectMovieId: Long, //收藏的电影id
                         ratingMovieId: Long, //评分的电影id
                         commentMovieId: Long, //评论的电影id
                         cityId: Long //城市id
                       )


  //用户信息表
  case class UserInfo(
                       userId: Long, //用户id
                       userName: String, //用户名
                       nickName: String, //用户昵称
                       age: Int, //用户年龄
                       professional: String, //职业
                       city: String, //城市
                       sex: Int //性别 1：男 0：女
                     )

  //session分析中top10的类别统计
  case class Top10CategoryStatistics(
                                      date: String,      //统计日期
                                      categoryId: Long,  //类别id
                                      clickCount: Int,   //该类别的点击次数
                                      collectCount: Int, //该类别的收藏次数
                                      ratingCount: Int,  //该类别的评分次数
                                      commentCount: Int  //该类别的评论次数
                                    )


  //城市信息样例类
  case class CityInfo(
                     id:Int,          //主键id
                     name:String,     //区域名称
                     pid:Int,         //区域的上级id，县的上级为市，市的上级为省，省的上级为国
                     sname:String,    //区域的简称
                     level:Int,       //级别，0：国 1：省 2：市 3：县
                     cityCode:Int,    //区域的编码，
                     yzCode:Int,      //邮政编码
                     mername:String,  //组合名称，如：中国,河北省,石家庄市,长安区
                     lng:Double,      //经度
                     lat:Double,      //纬度
                     pinyin:String    //区域名称的拼音

                     )


}
