package com.hnust.movie

import java.io.FileInputStream
import java.util.{Date, Properties, UUID}

import com.hnust.movie.SessionAnalyse.getClass
import com.hnust.movie.accumulator.SessionAggrStatAccumulator
import com.hnust.movie.entity.CaseClasses.{Top10CategoryStatistics, UserAction, UserInfo}
import com.hnust.movie.entity.Constant
import com.hnust.movie.utils.{DateUtil, UserStringUtils}
import org.apache.commons.lang.StringUtils
import org.apache.commons.lang.time.DateUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.storage.StorageLevel

import scala.collection.mutable

//统计结果的样例类
case class StatisticsInfo(date: String, statisticsMap: mutable.HashMap[String, Double])

//session的聚合信息样例类
case class AggrInfo(
                     sessionId: String, //session的id
                     searchKeywords: String, //用户一次session中的所有搜索关键词，用"-"分隔
                     clickCategoryIds: String, //用户一次session中的所有点击类别id，用"-"分隔
                     visitLength: Long, //用户一次session中的访问时长，
                     stepLength: Int, //用户一次session中的访问步长
                     startTime: Long, //session的开始时间
                     age: Int, //用户年龄
                     professional: String, //用户职业
                     city: String, //用户所在城市
                     sex: Int //用户性别 1：男 0：女
                   )

//统计top10的类别时用到的样例类，并继承Ordered类实现排序
case class CategorySortKey(clickCount: Int, collectCount: Int, ratingCount: Int, commentCount: Int) extends Ordered[CategorySortKey] {
  override def compare(that: CategorySortKey): Int = {

    if (this.clickCount - that.clickCount != 0) {
      return (this.clickCount - that.clickCount).toInt
    } else if (this.collectCount - that.collectCount != 0) {
      return (this.collectCount - that.collectCount).toInt
    } else if (this.ratingCount - that.ratingCount != 0) {
      return (this.ratingCount - that.ratingCount).toInt
    } else if (this.commentCount - that.commentCount != 0) {
      return (this.commentCount - that.commentCount).toInt
    }
    0

  }
}

case class Top10Session(date:String, categoryId:Long, userId:Long,sessionId:String, count:Int)

case class SessionDetail(
                          date:String,                 //统计的日期
                          userId:Long,                 //用户id
                          sessionId:String,            //session的id
                          pageId:String,               //页面的id
                          actionTime:Long,             //用户动作的时间
                          searchKeyword:String,        //搜索关键词
                          clickCategporyId:Long,       //点击类别id
                          collectionCategoryId:Long,   //收藏类别id
                          ratingCategoryId:Long,       //评分类别id
                          commentCategory:Long,        //评论类别id
                          clickMovieId:Long,           //点击的电影id
                          collectMovieId:Long,         //收藏的电影id
                          ratingMovieId:Long,          //评分的电影id
                          commentMovieId:Long,         //评论的电影id
                          cityId:Long                  //用户的城市id
                        )

/**
  * @Title:基于session会话的用户行为分析
  * 来源数据的格式：用户行为数据：动作日期|用户id|sessionId|页面id|查询词|点击类别id|点击电影id|收藏电影id|评分电影id|评论电影id|城市id
  *               用户表数据：用户id|用户名|昵称|年龄|职业|城市|性别
  * @Author: ggh
  * @Date: 2020/7/4 20:32
  */
object SessionAnalyse {


  def main(args: Array[String]): Unit = {

    //获取一个唯一的任务id
    val taskId: String = UUID.randomUUID().toString

    val sparkConf: SparkConf = new SparkConf().setAppName("SessionAnalyse").setMaster("local[*]")

    //获取sparkSession和sparkContext
    val sparkSession: SparkSession = SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate()
    val sc: SparkContext = sparkSession.sparkContext

    import sparkSession.implicits._

    //加载一下配置文件
    val path: String = getClass.getResource("/session.propertoes").getPath
    val prop: Properties = new Properties()
    prop.load(new FileInputStream(path))

    val userActionsRDD: RDD[UserAction] = getDataByDateRange(sparkSession, prop)

    val sessionId2ActionRDD: RDD[(String, UserAction)] = userActionsRDD.map {
      item => {
        (item.sessionId, item)
      }
    }

    //缓存
    //sessionId2ActionRDD.persist(StorageLevel.MEMORY_AND_DISK)

    //聚合，sessionid2AggrInfoRDD：(sessionId,聚合信息)，聚合信息包含时长，步长，开始时间等
    val sessionid2AggrInfoRDD: RDD[(String, AggrInfo)] = aggregateBySession(sessionId2ActionRDD, sparkSession)

    val sessionAggrStatAccumulator: SessionAggrStatAccumulator = new SessionAggrStatAccumulator

    //注册累加器
    sc.register(sessionAggrStatAccumulator, "sessionAggrStatAccumulator")

    //按照配置文件指定的过滤条件进行过滤(城市、年龄、职业、性别等)
    //filteredSessionid2AggrInfoRDD:(sessionId,聚合信息),
    //累加器的变化：里面含有session的个数，各访问时长的个数(如：(visit_length_1_3,456)...)、各访问步长的个数(如：(step_length_1_3,456)...)
    val filteredSessionid2AggrInfoRDD: RDD[(String, AggrInfo)] = filterSessionAndAggrStat(sessionid2AggrInfoRDD, sparkSession, prop, sessionAggrStatAccumulator)

    //对数据进行缓存
    //filteredSessionid2AggrInfoRDD.persist(StorageLevel.MEMORY_AND_DISK)

    //TODO:业务功能一：统计各个范围的session占比，并写入MySQL，
    //直接利用累加器中的数据进行计算
    calculateAndPersistAggrStat(sparkSession, sessionAggrStatAccumulator.value, taskId, prop)


    //TODO:业务功能三：获取top10的热门品类
    //将过滤出的数据 filteredSessionid2AggrInfoRDD 和原始用户行为数据 sessionId2ActionRDD 进行join，
    //目的就是得到符合过滤条件的原始用户行为数据,sessionid2detailRDD:(sessionId,userAction)
    val sessionid2detailRDD: RDD[(String, UserAction)] = filteredSessionid2AggrInfoRDD.join(sessionId2ActionRDD).map {
      case (sessionId, (aggrInfo, userAction)) => (sessionId, userAction)
    }

    //利用得到的数据提取出top10的类别信息,并保存到mongodb
    val top10CategoryStatisticses: Array[Top10CategoryStatistics] = getTop10Category(sparkSession, sessionid2detailRDD)


    //TODO:获取top10热门品类的活跃session
    getTop10Session(sparkSession,top10CategoryStatisticses,sessionid2detailRDD)

    sparkSession.stop()

  }


  /**
    * @title:
    * @description: 根据指定的日期范围从hive中获取数据
    * @param: sparkSession
    * @author:ggh
    * @updateTime: 2020/7/4 21:03
    **/
  def getDataByDateRange(sparkSession: SparkSession, prop: Properties) = {

    //从配置文件获取过滤的起始和结束日期
    val startDate: String = prop.getProperty("filter.start.date")
    val endDate: String = prop.getProperty("filter.end.date")

    import sparkSession.implicits._

    //将其转换成时间戳的形式
    val startTime: Long = DateUtil.dateStr2TimeMillis(startDate + " 00:00:00")
    val endTime: Long = DateUtil.dateStr2TimeMillis(endDate + " 00:00:00")

    //从hive读取数据，转换成UserAction格式的rdd
    sparkSession.sql("select * from user_visit_action where date >= " + startTime + " and date <= " + endTime)
      .as[UserAction]
      .rdd

  }

  /**
    * @title:
    * @description: 根据用户访问行为数据进行聚合,封装成样例类AggrInfo
    * @param: sessionId2ActionRDD：(sessionId,userAction)
    * @param: sparkSession
    * @author:ggh
    * @updateTime: 2020/7/5 9:47
    **/
  def aggregateBySession(sessionId2ActionRDD: RDD[(String, UserAction)], sparkSession: SparkSession) = {

    //按sessionId分组
    val sessionId2ActionsRDD: RDD[(String, Iterable[UserAction])] = sessionId2ActionRDD.groupByKey()

    //sessionId,searchKeywords, clickCategoryIds, visitLength,stepLength,
    //startTime,age,professional,city,sex

    //得到部分聚合信息,还不包含用户信息
    val partAggrInfo: RDD[(Long, AggrInfo)] = sessionId2ActionsRDD.map {
      case (sessionId, actions) => {

        //搜索关键词和点击类别id
        val searchKeywordsBuffer = new StringBuffer("")
        val clickCategoryIdsBuffer = new StringBuffer("")

        //用户id
        var userId = -1L

        //session的开始和结束时间
        var startTime: Long = 0L;
        var endTime: Long = 0L;

        //session的访问步长
        var stepLength: Int = 0
        //session的访问时长
        var visitLength: Long = 0L

        //对同一个session的所有用户操作进行遍历
        actions.foreach {
          action => {

            //获取userId
            if (userId == -1L) {
              userId = action.userId
            }

            //获取搜索关键词和点击类别id
            val searchKeyword: String = action.searchKeyword
            val clickCategoryId: Long = action.clickCategoryId

            //
            if (StringUtils.isNotBlank(searchKeywordsBuffer.toString)) {
              if (!searchKeywordsBuffer.toString.contains(searchKeyword)) {
                searchKeywordsBuffer.append(searchKeyword + "-")
              }
            }

            if (StringUtils.isNotEmpty(clickCategoryIdsBuffer.toString)) {
              if (!clickCategoryIdsBuffer.toString.contains(clickCategoryId)) {
                clickCategoryIdsBuffer.append(clickCategoryId + "-")
              }
            }

            //设置session的开始和结束时间
            if (action.date < startTime) {
              startTime = action.date
            }

            if (action.date > endTime) {
              endTime = action.date
            }

            //步长加1
            stepLength += 1

          }
        }

        //计算session的访问时长
        visitLength = endTime - startTime

        //去掉两边的 "-"
        val searchKeywords: String = UserStringUtils.trimCrossing(searchKeywordsBuffer.toString)
        val clickCategoryIds: String = UserStringUtils.trimCrossing(clickCategoryIdsBuffer.toString)


        //返回结果
        (userId, AggrInfo(sessionId, searchKeywords, clickCategoryIds, visitLength, stepLength, startTime, 0, null, null, null))

      }
    }

    //根据得到的部分聚合信息，和用户信息表进行join

    //获取用户表
    import sparkSession.implicits._
    //从hive中获取用户信息
    val userId2InfoRDD: RDD[(Long, UserInfo)] = sparkSession.sql("select * from user_info").as[UserInfo].rdd.map(info => (info.userId, info))

    //连接，整合用户信息
    val sessionId2FullAggrInfo: RDD[(String, AggrInfo)] = partAggrInfo.join(userId2InfoRDD).map {
      case (uid, (aggInfo, userInfo)) => {

        (aggInfo.sessionId, AggrInfo(aggInfo.sessionId, aggInfo.searchKeywords, aggInfo.clickCategoryIds,
          aggInfo.visitLength, aggInfo.stepLength, aggInfo.startTime, userInfo.age,
          userInfo.professional, userInfo.city, userInfo.sex))

      }
    }

    sessionId2FullAggrInfo

  }


  /**
    * @title:
    * @description: 按照年龄、职业、城市范围、性别、搜索词、点击品类这些条件进行过滤
    * @param: sessionid2AggrInfoRDD
    * @param: sparkSession
    * @param: prop
    * @author:ggh
    * @updateTime: 2020/7/5 10:43
    **/
  def filterSessionAndAggrStat(sessionid2AggrInfoRDD: RDD[(String, AggrInfo)],
                               sparkSession: SparkSession,
                               prop: Properties,
                               sessionAggrStatAccumulator: SessionAggrStatAccumulator) = {

    //从配置文件获取过滤的年龄范围、城市、职业、性别、电影类别等
    val filterAge: String = prop.getProperty("filter.age")
    val filterCities: String = prop.getProperty("filter.cities")
    val filterProfessional: String = prop.getProperty("filter.professional")
    val filterSex: String = prop.getProperty("filter.sex")
    val filterCategories: String = prop.getProperty("filter.categories")

    var ageSplit: Array[String] = null
    var startAge: String = null
    var endAge: String = null

    var filterCitiesList: List[String] = null
    var filterProfessionalList: List[String] = null
    var filterCategoriesList: List[String] = null

    if (StringUtils.isNotBlank(filterAge)) {
      ageSplit = filterAge.split("-")
      startAge = ageSplit(0)
      endAge = ageSplit(1)
    }

    if (StringUtils.isNotBlank(filterCities)) {
      filterCitiesList = filterCities.split("-").toList
    }

    if (StringUtils.isNotBlank(filterProfessional)) {
      filterProfessionalList = filterProfessional.split("-").toList
    }

    if (StringUtils.isNotBlank(filterCategories)) {
      filterCategoriesList = filterCategories.split("-").toList
    }


    //开始对数据进行过滤
    val filteredSessionid2AggrInfoRDD: RDD[(String, AggrInfo)] = sessionid2AggrInfoRDD.filter {
      case (sessionId, aggrInfo) => {

        var success: Boolean = true

        //过滤年龄
        if (StringUtils.isNotBlank(startAge) && StringUtils.isNotBlank(endAge)) {

          if (aggrInfo.age < startAge.toInt || aggrInfo.age > endAge.toInt) success = false

        }

        //城市过滤
        if (filterCitiesList != null) {

          if (!filterCitiesList.contains(aggrInfo.city)) success = false

        }

        //职业过滤
        if (filterProfessionalList != null) {
          if (!filterProfessionalList.contains(aggrInfo.professional)) success = false
        }

        //性别过滤
        if (StringUtils.isNotBlank(filterSex)) {
          if (!filterSex.equals(aggrInfo.sex)) success = false
        }

        //电影类别过滤
        if (filterCategoriesList != null) {

          val iterator: Iterator[String] = aggrInfo.clickCategoryIds.split("-").iterator

          //用于记录聚合信息的点击类别中是否包含过滤类别中的一个
          var flag: Boolean = false

          while (iterator.hasNext) {

            if (filterCategoriesList.contains(aggrInfo.clickCategoryIds)) {
              flag = true
            }
          }

          //聚合信息的点击类别中不包含过滤类别中的任何一个
          if (flag == false) success = false

        }

        //如果该条聚合信息符合所有过滤条件，就进行累加器的操作
        if (success) {

          //session的个数加1
          sessionAggrStatAccumulator.add("session_count")

          //统计访问时长
          //获取访问步长
          val visitLengthStr: String = prop.getProperty("session.visitLength")
          val visitLengthArray: Array[String] = visitLengthStr.split(",")

          import util.control.Breaks._
          val iterator: Iterator[String] = visitLengthArray.iterator

          breakable {
            while (iterator.hasNext) {
              val str: String = iterator.next() //格式：1_3
              val splits: Array[String] = str.split("-")
              val start = splits(0).toInt * 1000L //转换成毫秒
              val end = splits(1).toInt * 1000L //转换成毫秒

              //符合时间范围才做累加器操作
              if (aggrInfo.visitLength >= start && aggrInfo.visitLength <= end) {
                sessionAggrStatAccumulator.add("visit_length_" + str)
                break() //退出循环
              }

            }
          }


          //统计访问步长
          //获取访问步长的值
          val stepLengthStr: String = prop.getProperty("session.stepLength")
          val stepLengthArray: Array[String] = stepLengthStr.split(",")

          val stepIterator: Iterator[String] = stepLengthArray.iterator

          breakable {
            while (stepIterator.hasNext) {
              val str: String = stepIterator.next() //格式：1_3
              val splits: Array[String] = str.split("-")
              val start = splits(0).toInt
              val end = splits(1).toInt

              //符合步长范围才做累加器操作
              if (aggrInfo.stepLength >= start && aggrInfo.stepLength <= end) {
                sessionAggrStatAccumulator.add("step_length_" + str)
                break() //退出循环
              }

            }
          }


        }

        success
      }
    }

    //返回过滤结果
    filteredSessionid2AggrInfoRDD

  }


  /**
    * @title:
    * @description: 计算所有session中各访问时长、访问步长的占比，并保存到mongodb
    * @param: sparkSession
    * @param: value :map类型，里面包含个访问时长、访问步长的统计个数
    * @param: taskId
    * @author:ggh
    * @updateTime: 2020/7/5 17:29
    **/
  def calculateAndPersistAggrStat(sparkSession: SparkSession,
                                  value: mutable.HashMap[String, Int],
                                  taskId: String,
                                  prop: Properties) = {

    //保存各个访问时长、访问步长的占比,key为时长/步长名称（如visit_length_ratio_1_3
    //表示访问时长1-3s的占比）,value为其占比
    val resultMap: mutable.HashMap[String, Double] = mutable.HashMap[String, Double]()

    //从配置文件获取统计的访问时长和步长信息
    val visitLengthArray: Array[String] = prop.getProperty("session.visitLength").split("-")
    val stepLengthArray: Array[String] = prop.getProperty("session.stepLength").split("-")

    //获取总session的个数
    val sessionCount: Int = value.getOrElse("session_count", 0)

    var key: String = ""
    var tmpKey: String = ""

    //将session的总个数保存进map
    resultMap.put("session_count", sessionCount)

    //遍历求各个访问时长的占比，并保存的 resultMap 中
    visitLengthArray.foreach {
      line => {

        tmpKey = "visit_length_" + line
        //获取相应键的值
        val visitLength: Int = value.getOrElse(tmpKey, 0)
        //计算占比
        var visitLengthRatio: Double = 0.0
        if (visitLength > 0) {
          visitLengthRatio = visitLength.toDouble / sessionCount
        }

        //保存到map
        key = "visit_length_ratio_" + line

        resultMap.put(key, visitLengthRatio)

      }
    }

    //遍历计算各个访问步长的占比
    stepLengthArray.foreach {
      line => {

        tmpKey = "step_length_" + line

        //获取相应键的值
        val stepLength: Int = value.getOrElse(tmpKey, 0)

        var stepLengthRatio: Double = 0.0
        if (stepLength > 0) {
          stepLengthRatio = stepLength.toDouble / sessionCount
        }

        //保存到map
        key = "step_length_ratio_" + line

        resultMap.put(key, stepLengthRatio)

      }
    }

    //获取当前日期
    val date: String = DateUtil.formatByDate(new Date(), "yyyy-MM-dd HH:mm:ss")

    //封装成样例类
    val statisticsInfo = StatisticsInfo(date, resultMap)

    import sparkSession.implicits._

    //保存到mongodb
    sparkSession.sparkContext.makeRDD(Array(statisticsInfo))
      .toDF("date", "statisticsMap")
      .write
      .format("com.mongodb.spark.sql")
      .option("uri", Constant.MONGO_URI)
      .option("collection", Constant.SESSION_ANALYSE_MONGODB)
      .mode(SaveMode.Append)
      .save()


  }


  /**
    * @title:
    * @description: 统计top10的类别
    * @param: sparkSession
    * @param: sessionid2detailRDD
    * @author:ggh
    * @updateTime: 2020/7/5 22:09
    **/
  def getTop10Category(sparkSession: SparkSession, sessionid2detailRDD: RDD[(String, UserAction)]) = {

    //获取所有的类别并去重，转换成二元组：(类别id，类别id)
    val distinctCategoryIdRDD: RDD[(Long, Long)] = sessionid2detailRDD
      .filter { //把不是点击、收藏、评分、评论的数据过滤掉
        case (sessionId, userAction) => {
          var flag = false

          if (userAction.clickCategoryId != null || userAction.collectCategoryId != null || userAction.ratingCategoryId != null ||
            userAction.commentCategoryId != null) flag = true

          flag
        }
      }
      .map { //转换成二元组：(categoryId,categoryId)
        case (sessionId, action) => {

          if (action.clickCategoryId != null) {
            (action.clickCategoryId, action.clickCategoryId)
          } else if (action.collectCategoryId != null) {
            (action.collectCategoryId, action.collectCategoryId)
          } else if (action.ratingCategoryId != null) {
            (action.ratingCategoryId, action.ratingCategoryId)
          } else if (action.commentCategoryId != null) {
            (action.commentCategoryId, action.commentCategoryId)
          } else (0, 0)

        }
      }.distinct() //去重

    //分别计算各类别的点击、收藏、评分、评论次数
    val clickCategoryId2CountRDD: RDD[(Long, Int)] = getClickCategoryId2CountRDD(sessionid2detailRDD)
    val collectCategoryId2CountRDD: RDD[(Long, Int)] = getCollectCategoryId2CountRDD(sessionid2detailRDD)
    val ratingCategoryId2CountRDD: RDD[(Long, Int)] = getRatingCategoryId2CountRDD(sessionid2detailRDD)
    val commentCategoryId2CountRDD: RDD[(Long, Int)] = getCommentCategoryId2CountRDD(sessionid2detailRDD)

    import sparkSession.implicits._

    //将distinctCategoryIdRDD、clickCategoryId2CountRDD、collectCategoryId2CountRDD、
    //ratingCategoryId2CountRDD、commentCategoryId2CountRDD依次进行左外连接
    val clickJoinRDD: RDD[(Long, CategorySortKey)] = distinctCategoryIdRDD.leftOuterJoin(clickCategoryId2CountRDD).map {
      case (categoryId, (cid, optionValue)) => {

        val clickCount: Int = if (optionValue.isDefined) optionValue.get else 0

        (categoryId, CategorySortKey(clickCount, 0, 0, 0))

      }
    }

    val collectJoinRDD: RDD[(Long, CategorySortKey)] = clickJoinRDD.leftOuterJoin(collectCategoryId2CountRDD).map {
      case (categoryId, (categorySortKey, optionValue)) => {

        val collectCount: Int = if (optionValue.isDefined) optionValue.get else 0

        (categoryId, CategorySortKey(categorySortKey.clickCount, collectCount, 0, 0))

      }
    }

    val ratingJoinCountRDD: RDD[(Long, CategorySortKey)] = collectJoinRDD.leftOuterJoin(ratingCategoryId2CountRDD).map {
      case (categoryId, (categorySortKey, optionValue)) => {

        val ratingCount: Int = if (optionValue.isDefined) optionValue.get else 0

        (categoryId, CategorySortKey(categorySortKey.clickCount, categorySortKey.collectCount, ratingCount, 0))

      }
    }

    //最后得到的数据为：(类别id，(类别的点击次数，收藏次数，评分次数，评论次数))
    val commentJoinCountRDD: RDD[(Long, CategorySortKey)] = ratingJoinCountRDD.leftOuterJoin(commentCategoryId2CountRDD).map {
      case (categoryId, (categorySortKey, optionValue)) => {
        val commentCount: Int = if (optionValue.isDefined) optionValue.get else 0

        (categoryId, CategorySortKey(categorySortKey.clickCount, categorySortKey.collectCount, categorySortKey.ratingCount, commentCount))

      }
    }

    //获取当前日期
    val date: String = DateUtil.getCurrentTime("yyyy-MM-dd HH:mm:ss")

    val top10CategoryStatisticses: Array[Top10CategoryStatistics] = commentJoinCountRDD
      .sortBy(_._2)   //排序
      .take(10) //取前十个
      .map {
        case (categoryId, categorySortKey) => {

          Top10CategoryStatistics(date, categoryId, categorySortKey.clickCount, categorySortKey.collectCount, categorySortKey.ratingCount, categorySortKey.commentCount)
        }
      }

    //将其转换成rdd，再转换成DF
    val top10CategoriesDF: DataFrame = sparkSession.sparkContext.makeRDD(top10CategoryStatisticses).toDF()

    //保存到mongodb
    top10CategoriesDF.write
      .format("com.mongodb.spark.sql")
      .option("uri", Constant.MONGO_URI)
      .option("collection", Constant.SESSION_TOP10_CATEGORIES)
      .mode(SaveMode.Append)
      .save()

    top10CategoryStatisticses

  }


  /**
  *@title:
  *@description: 获取各类别的活跃session的统计与以及这些session详情数据
  *@param: sparkSession
  *@param: top10CategoryStatisticses：top10的类别统计信息,格式:(日期，类别id，该类别的点击次数，收藏次数，评分次数，评论次数)
  *@param: sessionid2detailRDD
  *@author:ggh
  *@updateTime: 2020/7/6 18:02
  **/
  def getTop10Session(
                       sparkSession: SparkSession,
                       top10CategoryStatisticses: Array[Top10CategoryStatistics],
                       sessionid2detailRDD: RDD[(String, UserAction)]) = {

    //从top10CategoryStatisticses中获取所有top10的类别id,并转换成二元组
    val categoryId2CategoryId: Array[(Long, Long)] = top10CategoryStatisticses.map(x => (x.categoryId, x.categoryId))

    val categoryId2CategoryIdRDD: RDD[(Long, Long)] = sparkSession.sparkContext.makeRDD(categoryId2CategoryId)

    //计算每一个session中包含的类别的点击次数:格式为：(类别id，"sessionId,点击次数" )
    //如(12, "33,44"),表示在sessionid为33的一系列用户行为操作中，对类别id为12的点击了44次
    val category2SessionCountRDD: RDD[(Long, String)] = sessionid2detailRDD.groupByKey().flatMap {
      case (sessionId, userActions) => {
        val categoryMap: mutable.HashMap[Long, Long] = mutable.HashMap[Long, Long]()

        //判断map里面是否已包含该类别
        userActions.foreach {
          action => {

            var categoryId: Long = 0L
            if (action.clickCategoryId != null && action.clickCategoryId > 0) categoryId = action.clickCategoryId
            if (action.collectCategoryId != null && action.collectCategoryId > 0) categoryId = action.collectCategoryId
            if (action.ratingCategoryId != null && action.ratingCategoryId > 0) categoryId = action.ratingCategoryId
            if (action.commentCategoryId != null && action.commentCategoryId > 0) categoryId = action.commentCategoryId

            //已包含就更新，加1
            if (categoryMap.contains(categoryId)) {
              categoryMap.update(categoryId, categoryMap(categoryId) + 1)
            } else {
              //没有就设置初值为1
              categoryMap += ((categoryId, 1))
            }

          }
        }
        val userId: Long = userActions.head.userId

        for ((categoryId, count) <- categoryMap)
          yield (categoryId, sessionId + "," + userId  +"," + count)

        //        List()
      }
    }

    import sparkSession.implicits._

    //将categoryId2CategoryIdRDD和category2SessionCountRDD进行join得
    //到category2SessionCountRDD为top10类别的数据
    val top10CategorySessionCountRDD: RDD[(Long, String)] =
    categoryId2CategoryIdRDD.join(category2SessionCountRDD).map {
      case (categoryId, (cid, line)) => {

        (categoryId, line)
      }
    }

    //获取下当前日期
    val date: String = DateUtil.getCurrentTime("yyyy-MM-dd HH:mm:ss")

    //分组、排序，每个类别取前50的session的数据
    val topSessionObjectRDD: RDD[Top10Session] = top10CategorySessionCountRDD.groupByKey().flatMap {
      case (categoryId, sessionId2CountStr) => {

        val sessionId2CountList: List[String] = sessionId2CountStr.toList.sortWith(_.split(",")(2) > _.split(",")(2)).take(50)

        sessionId2CountList.map(item => {
          val splits: Array[String] = item.split(",")

          Top10Session(date, categoryId, splits(1).toLong, splits(0), splits(2).toInt)

        })

        //        List()
      }
    }

    //保存到mongodb
    topSessionObjectRDD.toDF().write
      .format("com.mongodb.spark.sql")
      .option("uri",Constant.MONGO_URI)
      .option("collection",Constant.SESSION_TOP_SESSION_OF_CATEGORY_MONGODB)
      .mode(SaveMode.Append)
      .save()


    //同时将各类别的活跃session的详情数据也存到mongodb

    val sessionId2SessionId: RDD[(String, String)] = topSessionObjectRDD.map(item => (item.sessionId, item.sessionId))

    val sessionDetailDF: DataFrame = sessionId2SessionId.join(sessionid2detailRDD).map {
      case (sessionId, (sid, userAction)) => {

        SessionDetail(date, userAction.userId, sessionId, userAction.pageId, userAction.date,
          userAction.searchKeyword, userAction.clickCategoryId, userAction.collectCategoryId, userAction.ratingCategoryId,
          userAction.commentCategoryId, userAction.clickMovieId, userAction.collectMovieId, userAction.ratingMovieId,
          userAction.commentMovieId, userAction.cityId)
      }
    }.toDF()

    //保存到mongodb
    sessionDetailDF.write
      .format("com.mongodb.spark.sql")
      .option("uri", Constant.MONGO_URI)
      .option("collection",Constant.SESSION_TOP_SESSION_DETAIL_OF_CATEGORY_MONGODB)
      .mode(SaveMode.Append)
      .save()


  }

  /**
    * @title:
    * @description: 统计各类别的点击次数
    * @param: sessionid2detailRDD ： (sessionId,userAction)
    * @author:ggh
    * @updateTime: 2020/7/5 20:35
    **/
  def getClickCategoryId2CountRDD(sessionid2detailRDD: RDD[(String, UserAction)]) = {

    //过滤出点击行为的数据,并转换成二元组再求和
    sessionid2detailRDD.filter(_._2.clickCategoryId != null).map {
      case (sessionId, userAction) => {
        (userAction.clickCategoryId, 1)
      }
    }.reduceByKey(_ + _)


  }

  /**
    * @title:
    * @description: 统计个类别的收藏次数
    * @param: sessionid2detailRDD： (sessionId,userAction)
    * @author:ggh
    * @updateTime: 2020/7/5 20:35
    **/
  def getCollectCategoryId2CountRDD(sessionid2detailRDD: RDD[(String, UserAction)]) = {

    //过滤出收藏行为的数据,并转换成二元组再求和
    sessionid2detailRDD.filter(_._2.collectCategoryId != null).map {
      case (sessionId, userAction) => {
        (userAction.collectCategoryId, 1)
      }
    }.reduceByKey(_ + _)


  }

  /**
    * @title:
    * @description: 统计各类别的评分次数
    * @param: sessionid2detailRDD:： (sessionId,userAction)
    * @author:ggh
    * @updateTime: 2020/7/5 20:36
    **/
  def getRatingCategoryId2CountRDD(sessionid2detailRDD: RDD[(String, UserAction)]) = {

    //过滤出评分行为的数据,并转换成二元组再求和
    sessionid2detailRDD.filter(_._2.ratingCategoryId != null).map {
      case (sessionId, userAction) => (userAction.ratingCategoryId, 1)
    }.reduceByKey(_ + _)


  }

  /**
    * @title:
    * @description: 统计各类别的评论次数
    * @param: sessionid2detailRDD： (sessionId,userAction)
    * @author:ggh
    * @updateTime: 2020/7/5 20:36
    **/
  def getCommentCategoryId2CountRDD(sessionid2detailRDD: RDD[(String, UserAction)]) = {

    //过滤出评论行为的数据,并转换成二元组再求和
    sessionid2detailRDD.filter(_._2.commentCategoryId != null).map {
      case (sessionId, userAction) => (userAction.commentCategoryId, 1)
    }.reduceByKey(_ + _)

  }

}
