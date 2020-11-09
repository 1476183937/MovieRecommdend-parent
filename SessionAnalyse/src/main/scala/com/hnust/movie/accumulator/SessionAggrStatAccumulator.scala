package com.hnust.movie.accumulator

import org.apache.spark.util.AccumulatorV2

import scala.collection.mutable

/**
  * @Title:自定义的累加器
  * @Author: ggh
  * @Date: 2020/7/5 10:56
  */
class SessionAggrStatAccumulator extends AccumulatorV2[String, mutable.HashMap[String,Int]]{

  //保存所有聚合信息
  //统计所有session中各个访问时长、访问步长的个数
  private val aggrStatMap: mutable.HashMap[String, Int] = mutable.HashMap[String,Int]()

  override def isZero: Boolean = {

    aggrStatMap.isEmpty

  }

  override def copy(): AccumulatorV2[String, mutable.HashMap[String, Int]] = {

    val newAcc = new SessionAggrStatAccumulator

    aggrStatMap.synchronized{
      newAcc.aggrStatMap ++= this.aggrStatMap
    }
    newAcc

  }

  override def reset(): Unit = {
    aggrStatMap.clear()
  }

  override def add(v: String): Unit = {

    //如果aggrStatMap不包含该键，就添加该键，设置值为0
    if (!aggrStatMap.contains(v)){
      aggrStatMap += (v -> 0)
    }
    //对该键的值加1
    aggrStatMap.update(v, aggrStatMap.getOrElse(v,0) + 1)

  }

  //合并两个map
  override def merge(other: AccumulatorV2[String, mutable.HashMap[String, Int]]): Unit = {
    other match {
      case acc:SessionAggrStatAccumulator => {
        (this.aggrStatMap /: acc.value){ case (map, (k,v)) => map += ( k -> (v + map.getOrElse(k, 0)) )}
      }
    }
  }

  override def value: mutable.HashMap[String, Int] = {

    this.aggrStatMap

  }
}
