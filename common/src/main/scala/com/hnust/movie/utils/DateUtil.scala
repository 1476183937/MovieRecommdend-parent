package com.hnust.movie.utils

import java.text.SimpleDateFormat
import java.util.{Calendar, Date, GregorianCalendar}

/**
  * @Title:
  * @Author: ggh
  * @Date: 2020/6/3 11:06
  */
object DateUtil {


  /**
  *@title:
  *@description: 根据传入的日期获取指定格式的日期字符串
  *@param: date
  *@author:ggh
  *@updateTime: 2020/6/19 20:40
  **/
  def formatByDate(date:Date,format:String)  ={

    new SimpleDateFormat(format).format(date)

  }

  //获取指定日期的星期内的星期一
  def getMondayByDate(date:Date): String ={

    val calendar: Calendar = Calendar.getInstance()

    calendar.setFirstDayOfWeek(Calendar.MONDAY)
    calendar.setTime(date)
    calendar.set(Calendar.DAY_OF_WEEK,calendar.getFirstDayOfWeek)


    val dateFormat = new SimpleDateFormat("yyyy-MM-dd")
    dateFormat.format(calendar.getTime)

  }

  //获取指定日期的星期内的星期天
  def getSundayByDate(date:Date): String ={

    val calendar: Calendar = Calendar.getInstance()

    calendar.setFirstDayOfWeek(Calendar.MONDAY)
    calendar.setTime(date)
    calendar.set(Calendar.DAY_OF_WEEK,calendar.getFirstDayOfWeek+6)


    val dateFormat = new SimpleDateFormat("yyyy-MM-dd")
    dateFormat.format(calendar.getTime)
  }

  //将字符串形式的日期转换成毫秒值
  def dateStr2TimeMillis(dateStr:String): Long ={
    //2020-12-23 12:20:23 或 2020-12-23

    var result = 0l

    if (dateStr.length == 10){
      val timeSplit: Array[String] = dateStr.split("-")

//      val date = new Date(timeSplit(0).toInt-1900,timeSplit(1).toInt,timeSplit(2).toInt)
      val calendar: Calendar = Calendar.getInstance()
      //年
      calendar.set(Calendar.YEAR,timeSplit(0).toInt)
      //月
      if (timeSplit(1).startsWith("0")){

        calendar.set(Calendar.MONTH,timeSplit(1).substring(1,2).toInt-1)
      }else {
        calendar.set(Calendar.MONTH,timeSplit(1).toInt-1)
      }

      //日
      if (timeSplit(2).startsWith("0")){
        calendar.set(Calendar.DAY_OF_MONTH,timeSplit(2).substring(1,2).toInt)
      }else {
        calendar.set(Calendar.DAY_OF_MONTH, timeSplit(2).toInt)
      }

//      calendar.set(Calendar.HOUR_OF_DAY,13)
//      calendar.set(Calendar.MINUTE,5)
//      calendar.set(Calendar.SECOND,12)

      result = calendar.getTimeInMillis

    }
    if (dateStr.length == 19){
      val timeSplit: Array[String] = dateStr.split(" ")
      val timeSplit1: Array[String] = timeSplit(0).split("-")
      val timeSplit2: Array[String] = timeSplit(1).split(":")

      val calendar: Calendar = Calendar.getInstance()
      //年
      calendar.set(Calendar.YEAR,timeSplit1(0).toInt)

      //月
      if (timeSplit1(1).startsWith("0")){

        calendar.set(Calendar.MONTH,timeSplit1(1).substring(1,2).toInt-1)
      }else {
        calendar.set(Calendar.MONTH,timeSplit1(1).toInt-1)
      }

      //日
      if (timeSplit1(2).startsWith("0")){
        calendar.set(Calendar.DAY_OF_MONTH,timeSplit1(2).substring(1,2).toInt)
      }else {
        calendar.set(Calendar.DAY_OF_MONTH, timeSplit1(2).toInt)
      }

      //时
      if (timeSplit2(0).startsWith("0")){
        calendar.set(Calendar.HOUR_OF_DAY,timeSplit2(0).substring(1,2).toInt)
      }else {
        calendar.set(Calendar.HOUR_OF_DAY, timeSplit2(0).toInt)
      }

      //分
      if (timeSplit2(1).startsWith("0")){
        calendar.set(Calendar.MINUTE,timeSplit2(1).substring(1,2).toInt)
      }else {
        calendar.set(Calendar.MINUTE, timeSplit2(1).toInt)
      }

      //秒
      if (timeSplit2(2).startsWith("0")){
        calendar.set(Calendar.SECOND,timeSplit2(2).substring(1,2).toInt)
      }else {
        calendar.set(Calendar.SECOND, timeSplit2(2).toInt)
      }

      result = calendar.getTimeInMillis
    }

    result
  }

  /**
  *@title:
  *@description: 根据指定格式获取当前时间
  *@param: format
  *@author:ggh
  *@updateTime: 2020/6/3 19:12
  **/
  def getCurrentTime(format: String): String ={
    new SimpleDateFormat(format).format(new Date())
  }

  /**
  *@title:
  *@description: 计算指定日期到当前时间的小时数
  *@param: date
  *@author:ggh
  *@updateTime: 2020/6/3 11:41
  **/
  def timeDiffHoursToNow(date:String): Int ={

    val preDate = dateStr2TimeMillis(date)

    val nowTime: Long = new Date().getTime

    ((nowTime - preDate)/1000/3600).toInt

  }


  def main(args: Array[String]): Unit = {

//    println((dateStr2TimeMillis("2020-06-03 10:30:33") - dateStr2TimeMillis("2020-06-03 10:30:32")))

   /* val calendar: Calendar = Calendar.getInstance()

    calendar.set(Calendar.YEAR,2020)
    calendar.set(Calendar.MONTH,5)
    calendar.set(Calendar.DAY_OF_MONTH,3)
    calendar.set(Calendar.HOUR_OF_DAY,13)
    calendar.set(Calendar.MINUTE,18)
    calendar.set(Calendar.SECOND,15)

    println(calendar.getTimeInMillis)*/

//    println(dateStr2TimeMillis("2020-06-03 13:20:40"))
//
//    println(new Date().getTime)
//
//    println(timeDiffHoursToNow("2020-06-02 11:26:30"))

//    dateDecrease()

    println(getMondayByDate(new Date()))
    println(getSundayByDate(new Date()))

  }

}
