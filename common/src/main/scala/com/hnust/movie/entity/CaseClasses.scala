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
  case class Rating_DB(rating_id:Long, uid:Long,mid:Long,rating:Double, date:String)

  //数据库中电影详情表的样例类
  case class MovieInfo_DB(mid:Long ,
                       movie_name:String ,
                       img_urls:String ,
                       directors:String ,
                       screen_writer:String ,
                       main_actors:String ,
                       categories:String ,
                       location:String ,
                       language:String ,
                       release_date:String ,
                       run_time:String,
                       alias:String ,
                       summery:String,
                       rating_num:Double ,
                       off_shelf:Int ,
                       hot_degree:Int,
                       view_num:Long ,
                       play_url:String )

  //数据库中电影评论表的样例类
  case class Comment_DB(comment_id:String,
                        uid:Long, mid:Long,
                        content:String,
                        date:String,
                        deleted:Int,
                        like_num:Int,
                        dislike_num:Int)


  //数据库中用户浏览历史的样例类
  case class ScanHistory_DB(scan_id:String,
                            uid:Long,
                            mid:Long,
                            movie_name:String,
                            image:String,
                            date:String,
                            deleted:Int)

  //数据库中用户收藏表的名称
  case class UserCollection_DB(collection_id:String,
                               mid:Long,
                               uid:Long,
                               image:String,
                               movie_name:String,
                               date:String,
                               deleted:Int)

  //电影统计信息在mongodb中的存储格式
//  case class Movie_MongoDB(mid:Long,movie_name:String,hot_degree:Double,imgUrl:String,ratingNum:Double,categories:String)
  case class Movie_MongoDB(mid:Long,movieName:String,hotDegree:Double,imgUrls:String,ratingNum:Double,categories:String)

  //在mongodb中存储类别统计信息格式
  case class CategoryStatistics(category:String, time:String,movieList: Seq[Movie_MongoDB])

  //在mongodb中存储电影排行榜前十的统计信息格式
  case class TOP_MOVIES_MONGODB(date:String,movieList: Seq[Movie_MongoDB])

  //在mongodb中存储动漫排行榜前十的统计信息格式
  case class TOP_COMICS_MONGODB(date:String,movieList: Seq[Movie_MongoDB])

  //综合排行榜的样例类，包含热播榜，北美榜、大陆榜、好评榜，用category来区别这四种
  case class MultipleRanking(date:String, category:String,movieList:Seq[Movie_MongoDB])

  //在mongodb中存储本月排行榜前十的统计信息格式
  case class TopMoviesOfMonth(date:String,movieList:Seq[Movie_MongoDB])

  //在mongodb中存储本周排行榜前十的统计信息格式
  case class TopMoviesOfWeek(date:String,movieList:Seq[Movie_MongoDB])

  //电影推荐的存储格式
  case class MovieResc(mid:Long,
                       movieName:String,
                       hotDegree:Double,
                       imgUrls:String,
                       ratingNum:Double,
                       categories:String,
                       score:Double)

  //在mongodb中存储每个电影下与其相似的电影的数据格式
  case class SimilarMovieRecommendation(date:String, mid:Long, simliarMovies:Seq[MovieResc])

  //
  case class Mid2Score(mid:Long,score:Double)

  //存放电影相似度矩阵：mid2Score表示每一个电影和电影id为mid的电影的相似度
  case class Mid2SimilarScore(date:String,mid:Long,mid2Score:Seq[Mid2Score])
//  case class Mid2SimilarScoreList(date:String,mid:Long,mid2Score:List[Mid2Score])

  //mongodb中用户推荐电影collection的存储格式
  case class UserRecommendation(date:String,uid:Long,userResc:Seq[MovieResc])

  //数据库中电影的详情表的存储格式
  case class MoveiInfo_DB(
              mid:Long,
              movieName:String,
              imgUrls:String,
              directors:String,
              screenWriter:String,
              mainActors:String,
              categories:String,
              location:String,
              language:String,
              releaseDate:String,
              runTime:String,
              alias:String,
              summery:String,
              ratingNum:Float,
              offShelf:Integer,
              hotDegree:Integer,
              viewNum:Long,
              playUrl:String,
              extend2:String
  )

  //登录时间样例类：eventType表示登录成功(success)或失败(fail)
  case class LoginEvent(uid:Long,ip:String,eventType:String,timestamp:Long)

}
