package com.hnust.movie

import java.util.Date

import com.hnust.movie.utils.DateUtil

/**
  * @Title:
  * @Author: ggh
  * @Date: 2020/6/19 10:17
  */
object Test {

  def main(args: Array[String]): Unit = {
    val time: Long = new Date().getTime

    println(time)

    println(DateUtil.getCurrentTime("yyyy-MM-dd-HH"))

    val start = DateUtil.getCurrentTime("yyyy-MM-dd-HH")

    println(start.substring(0, start.length - 1)+(start.substring(start.length - 1).toInt + 1).toString)

    println(start.substring(start.length - 1).toInt + 1)

    println(start.charAt(start.length - 1).toInt + 1)

    println(Math.pow(2, 24).toString)

  }

}
