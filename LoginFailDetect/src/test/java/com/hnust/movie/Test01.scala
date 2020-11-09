package com.hnust.movie


/**
  * @Title:
  * @Author: ggh
  * @Date: 2020/7/4 10:46
  */
object Test01 {

  def main(args: Array[String]): Unit = {
    println("fjd,sakl".split(",").length)


    import util.control.Breaks._
    val list1 = List(1, 2, 3, 4, 5)

    val iterator = list1.iterator

    breakable{
      while (iterator.hasNext){

        val i = iterator.next()

        if (i == 4) break() else println(i)
      }
    }



    //breakable方法与break方法组合使用实现break功能
    //将整个循环放置于breakable方法中，然后需要跳出循环的时候则使用break方法，则跳出整个循环
    breakable {
      println("break功能展示：——————————————")

      for (i <- list1) {
        if (i == 4) break else println(i)
      }

    }
  }

}

