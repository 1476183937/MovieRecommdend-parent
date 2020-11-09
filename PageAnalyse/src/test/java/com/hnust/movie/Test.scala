package com.hnust.movie

import scala.collection.mutable.ListBuffer

/**
  * @Title:
  * @Author: ggh
  * @Date: 2020/7/8 16:52
  */
object Test {

  def main(args: Array[String]): Unit = {

    val matchList: ListBuffer[String] = ListBuffer()

    matchList += "index_detail"  //首页到详情页
    matchList += "detail_comment"//详情页到评论
    matchList += "detail_rating" //详情页到评分
    matchList += "detail_collect"//详情页到收藏
    matchList += "detail_play"   //详情页到播放页
    matchList.append("kkkkk")

    matchList.foreach(println)


  }

}
