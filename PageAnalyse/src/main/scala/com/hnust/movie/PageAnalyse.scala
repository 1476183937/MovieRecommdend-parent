package com.hnust.movie

import java.io.FileInputStream
import java.util.Properties

import com.hnust.movie.entity.CaseClasses.UserAction
import com.hnust.movie.entity.Constant
import com.hnust.movie.utils.DateUtil
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{SaveMode, SparkSession}

import scala.collection.mutable.ListBuffer


case class PageConvertRate(date: String, indexToDetail: Double, detailToRating: Double, detailToComment: Double, detailToCollect: Double, detailToPlay: Double)

/**
  * @Title:页面分析
  * 分析首页到详情页的转化率，详情页到评论的转化率，详情页到评分的转化率，
  * 详情页到收藏的转化率，详情页到播放页的转化率
  * @Author: ggh
  * @Date: 2020/7/8 16:28
  */
object PageAnalyse {


  def main(args: Array[String]): Unit = {

    val sparkConf: SparkConf = new SparkConf().setAppName("PageAnalyse").setMaster("local[*]")

    val spark: SparkSession = SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate()
    val sc: SparkContext = spark.sparkContext

    val prop: Properties = new Properties()
    val path: String = getClass.getResource("/page.properties").getPath

    prop.load(new FileInputStream(path))

    //获取指定日期内的数据
    val userActionRDD: RDD[UserAction] = getDataByRangeDate(prop, spark)

    //转换成格式：(sessionId,用户行为数据)
    val sessionId2ActionRDD: RDD[(String, UserAction)] = userActionRDD.map {
      action => (action.sessionId, action)
    }

    //分组
    val sessionId2ActionsRDD: RDD[(String, Iterable[UserAction])] = sessionId2ActionRDD.groupByKey()

    //matchedRDDs：((index_detail,1),(detail_comment,1),...)
    val matchedRDDs: RDD[(String, Int)] = generateAndMatchPageSplit(sessionId2ActionsRDD, spark)

    //统计每一个key对应的个数
    val countMap: collection.Map[String, Long] = matchedRDDs.countByKey()

    //获取首页的访问数量
    val indexPageCount: Long = getIndexPageCount(sessionId2ActionsRDD)

    //计算各页面的转换率并保存到mongodb
    computePageSplitConvertRate(countMap, spark: SparkSession)

    sc.stop()

  }


  /**
    * @title:
    * @description: 根据配置文件的条件获取指定日期内的数据
    * @param:
    * @author:ggh
    * @updateTime: 2020/7/8 16:30
    **/
  def getDataByRangeDate(prop: Properties, spark: SparkSession) = {

    val startDate: String = prop.getProperty("filter.start.date")
    val endDate: String = prop.getProperty("filter.end.date")

    val startTime: Long = DateUtil.dateStr2TimeMillis(startDate + " 00:00:00")
    val endTime: Long = DateUtil.dateStr2TimeMillis(endDate + " 00:00:00")

    import spark.implicits._

    spark.sql("select * from user_visit_action where date >= " + startTime + " and date <= " + endTime)
      .as[UserAction]
      .rdd

  }

  /**
    * @title:
    * @description: 得到首页到详情页，详情页到评论，详情页到评分，详情页到收藏，详情页到播放页的统计
    *               结果格式为:(index_detail,1),(detail_comment,1),...
    * @param: sessionId2ActionsRDD
    * @param: spark
    * @author:ggh
    * @updateTime: 2020/7/8 17:13
    **/
  def generateAndMatchPageSplit(sessionId2ActionsRDD: RDD[(String, Iterable[UserAction])], spark: SparkSession): RDD[(String, Int)] = {

    //创建匹配的list
    val matchList: ListBuffer[String] = ListBuffer()

    matchList += "index_detail" //首页到详情页
    matchList += "detail_comment" //详情页到评论
    matchList += "detail_rating" //详情页到评分
    matchList += "detail_collect" //详情页到收藏
    matchList += "detail_play" //详情页到播放页

    //广播出去
    val matchListBd: Broadcast[ListBuffer[String]] = spark.sparkContext.broadcast(matchList)

    //
    sessionId2ActionsRDD.flatMap {
      case (sessionId, actions) => {

        val resultList: ListBuffer[(String, Int)] = ListBuffer()

        //对actions进行按时间从小到大排序
        val sortedActions: List[UserAction] = actions.toList.sortWith(_.date < _.date)

        //提取出actions中的所有pageId
        val allPageIds: List[String] = sortedActions
          .filter(_.pageId != null)
          .filter(action => (action.pageId.contains("detail") ||
            action.pageId.contains("index") ||
            action.pageId.contains("comment") ||
            action.pageId.contains("rating") ||
            action.pageId.contains("collect") ||
            action.pageId.contains("play")
            )) //把属于访问首页、详情页、评论、评分、收藏、播放等操作的数据过滤出来
          .map(_.pageId)

        val allPageIdsList: List[String] = allPageIds.map {
          pageId => {
            if (pageId.contains("detail")) "detail"
            else if (pageId.contains("index")) "index"
            else if (pageId.contains("comment")) "comment"
            else if (pageId.contains("rating")) "rating"
            else if (pageId.contains("collect")) "collect"
            else "play"
          }
        }


        allPageIdsList
          .slice(0, allPageIds.length - 1)
          .zip(allPageIds.tail) //拉链操作
          .map(item => item._1 + "_" + item._2) //转换成格式：(index_detail,detail_comment,comment_detail,detail_play,...)
          .filter(matchListBd.value.contains(_)) //过滤出index_detail，detail_comment，detail_rating，detail_collect,detail_play的
          .map((_, 1)) //转换成格式((index_detail,1),(detail_comment,1),...)
          .foreach(resultList.append(_)) //添加到resultList里


        //计算首页和详情页的访问数量
        allPageIdsList
          .filter(pageId => "index".equals(pageId) || "detail".equals(pageId))
          .map((_, 1))
          .foreach(resultList.append(_)) //添加到resultList里

        resultList
      }
    }


  }


  /**
    * @title:
    * @description: 获取首页的访问数量
    * @param: sessionId2ActionsRDD
    * @author:ggh
    * @updateTime: 2020/7/8 17:24
    **/
  def getIndexPageCount(sessionId2ActionsRDD: RDD[(String, Iterable[UserAction])]) = {

    sessionId2ActionsRDD.flatMap {
      case (sessionId, actions) => {

        actions.filter(_.pageId.contains("index")).map(x => "index")

      }
    }.count()

  }

  /**
    * @title:
    * @description: 计算页面之间的转化率
    * @param: countMap：格式如：((index_detail,457),(detail_play,786),(detail_comment,347),...)
    * @param: indexPageCount:首页的访问数量
    * @author:ggh
    * @updateTime: 2020/7/8 17:26
    **/
  def computePageSplitConvertRate(countMap: collection.Map[String, Long], spark: SparkSession) = {

    //获取首页的访问数量
    val indexPageCount: Long = countMap("index")

    //计算首页到详情页的转换率
    val indexToDetailCount: Double = countMap("index_detail").toDouble
    val indexToDetailRate: Double = indexToDetailCount / indexPageCount

    //获取详情页的访问数量
    val detailPageCount: Double = countMap("detail").toDouble

    //计算详情页到评分的转化率
    val detailToRatingCount: Double = countMap("detail_rating").toDouble
    val detailToRatingRate: Double = detailToRatingCount / detailPageCount

    //计算详情页到评论的转化率
    val detailToCommentCount: Long = countMap("detail_comment")
    val detailToCommentRate: Double = detailToCommentCount / detailPageCount

    //计算详情页到收藏的转化率
    val detailToCollectCount: Long = countMap("detail_collect")
    val detailToCollectRate: Double = detailToCommentCount / detailToCollectCount

    //计算详情页到播放页的转化率
    val detailToPlayCount: Long = countMap("detail_collect")
    val detailToPlayRate: Double = detailToCommentCount / detailToPlayCount

    //获取当前日期
    val date: String = DateUtil.getCurrentTime("yyyy-MM-dd HH:mm:ss")

    //封装成样例类
    val pageConvertRate = PageConvertRate(date, indexToDetailRate, detailToRatingRate, detailToCommentRate, detailToCollectRate, detailToPlayRate)

    import spark.implicits._

    //保存到mongodb
    spark.sparkContext.makeRDD(Array(pageConvertRate))
      .toDF()
      .write
      .format("com.mongodb.spark.sql")
      .option("uri", Constant.MONGO_URI)
      .option("collection", Constant.PAGE_CONVERT_RATE_MONGODB)
      .mode(SaveMode.Append)
      .save()

  }


}
