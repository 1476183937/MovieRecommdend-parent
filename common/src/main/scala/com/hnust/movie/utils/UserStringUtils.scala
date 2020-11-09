package com.hnust.movie.utils

/**
  * @Title:
  * @Author: ggh
  * @Date: 2020/7/5 10:17
  */
object UserStringUtils {

  /**
    * 截断字符串两侧的横线 "-"
    * @param str 字符串
    * @return 字符串
    */
  def trimCrossing(str:String):String = {
    var result = ""
    if(str.startsWith("-")) {
      result = str.substring(1)
    }
    if(str.endsWith("-")) {
      result = str.substring(0, str.length() - 1)
    }
    result
  }

}
