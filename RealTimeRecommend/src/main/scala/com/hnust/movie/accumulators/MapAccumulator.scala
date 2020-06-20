package com.hnust.movie.accumulators

import java.util

import org.apache.spark.util.AccumulatorV2

import scala.collection.mutable

/**
  * @Title:自定义的map累加器
  * @Author: ggh
  * @Date: 2020/6/13 17:02
  */
class MapAccumulator extends AccumulatorV2[(Long,Map[Long,Double]),mutable.HashMap[Long,Map[Long,Double]]]{

  private var resultMap = new mutable.HashMap[Long,Map[Long,Double]]()

  override def isZero: Boolean = resultMap.isEmpty

  override def copy(): AccumulatorV2[(Long, Map[Long, Double]), mutable.HashMap[Long, Map[Long, Double]]] = new MapAccumulator()

  override def reset(): Unit = resultMap.clear()

  override def add(v: (Long, Map[Long, Double])): Unit = {
    resultMap+=(v)
//    println("添加后："+resultMap.take(10))
  }

  override def merge(other: AccumulatorV2[(Long, Map[Long, Double]), mutable.HashMap[Long, Map[Long, Double]]]): Unit = {

    for (elem <- other.value) {
      resultMap+=elem
    }

//    resultMap ++ other.value
  }

  override def value: mutable.HashMap[Long, Map[Long, Double]] = {
    resultMap
  }
}
