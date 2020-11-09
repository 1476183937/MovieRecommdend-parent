package com.hnust.movie

import java.text.SimpleDateFormat
import java.util.Date

import com.hnust.movie.utils.DateUtil

object Test {

  def main(args: Array[String]): Unit = {
    val time: Long = new Date().getTime

    println(time)

    //1592732512646
    println(DateUtil.formatByDate(new Date(1592732512646L), "yyyy-MM-dd HH:mm:ss"))
    println(DateUtil.formatByDate(new Date(1592732572646L), "yyyy-MM-dd HH:mm:ss"))
    println(DateUtil.formatByDate(new Date(1592732572646L+1000*60*19), "yyyy-MM-dd HH:mm:ss")+"--" + (1592732572646L+1000*60*19))
    println(DateUtil.formatByDate(new Date(1592736172646L), "yyyy-MM-dd HH:mm:ss"))

    println(DateUtil.getCurrentTime("yyyy-MM-dd-HH"))

    /*val start = DateUtil.getCurrentTime("yyyy-MM-dd-HH")

    println(start.substring(0, start.length - 1)+(start.substring(start.length - 1).toInt + 1).toString)

    println(start.substring(start.length - 1).toInt + 1)

    println(start.charAt(start.length - 1).toInt + 1)

    println(Math.pow(2, 24).toString)*/

  }

}