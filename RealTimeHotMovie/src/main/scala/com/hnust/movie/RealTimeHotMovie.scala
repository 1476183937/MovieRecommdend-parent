package com.hnust.movie

import java.io.FileInputStream
import java.util.{Date, Properties}
import java.{lang, util}

import org.apache.flink.api.common.functions.{AggregateFunction, RichAggregateFunction}
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor, MapState, MapStateDescriptor}
import org.apache.flink.api.java.tuple.{Tuple, Tuple1}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.{ProcessWindowFunction, WindowFunction}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.util.Collector
import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util.Properties
import java.util.concurrent.atomic.AtomicInteger

import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.windowing.triggers.{Trigger, TriggerResult}
import org.apache.kafka.clients.consumer.ConsumerConfig

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

case class LogEvent(uid: Long, ip: String, mid: Long, event: String, timestamp: Long)

case class MovieCount(mid: Long, windowEnd: Long, count: Long)

/**
  * @Title:实时热门电影统计,根据播放量来进行统计，flink实现
  * * 用户行为日志格式：uid|ip|mid|event|timestamp,电影id（mid）和用户id（uid）是可能为空的
  * * event的值有:detail(浏览详情页)、play(进入播放页)、fav(收藏操作)、rating(评分操作)、
  * * search(搜索操作)、index(进入首页)、comment(评论操作)、login(登录操作)、
  * @Author: ggh
  * @Date: 2020/6/21 11:03
  */
object RealTimeHotMovie {

  def main(args: Array[String]): Unit = {

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    //定义kafka的配置
    val path: String = getClass.getResource("/application.properties").getPath

    val prop = new Properties()

    prop.load(new FileInputStream(path))

    val kafkaBrokers: String = prop.getProperty("kafka.broker.servers")

    prop.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBrokers)
    prop.put(ConsumerConfig.GROUP_ID_CONFIG, "event")
    prop.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer")
    prop.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer")
    prop.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest")

    //获取kafka的stream流
    val kafkaConsumer = new FlinkKafkaConsumer[String]("event", new SimpleStringSchema(), prop)

    val kafkaStream: DataStream[String] = env.addSource(kafkaConsumer)

//    kafkaStream.join(kafkaStream).where().equalTo().window()
    //
    kafkaStream
      .filter(_.split("\\|").length == 5)
      .map(line => {
        val strings: Array[String] = line.split("\\|")

        LogEvent(strings(0).toLong, strings(1), strings(2).toLong, strings(3), strings(4).toLong)

      })
      .filter(_.event.equals("play")) //过滤出事件类型为播放的时间
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[LogEvent](Time.seconds(30)) {
      override def extractTimestamp(element: LogEvent) = element.timestamp
    })
      .keyBy(_.mid) //根据电影id分组
      //      .print("key by:")
      .timeWindow(Time.hours(1), Time.minutes(5)) //滑动窗口大小为1小时，每5分钟滑动一次
//      .trigger(new EventTrigger())
        .process(new TopNMovies())
//        .trigger()
//      .process()
//      .aggregate(new PreAggr(),new ResultCount())
//      .keyBy(_.windowEnd)
//      .process(new TopNMovies())



    env.execute("start:---")

  }

  //自定义触发器，来一条数据就触发一次计算
  class EventTrigger extends Trigger[LogEvent, TimeWindow] {
    override def onElement(element: LogEvent, timestamp: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult = TriggerResult.FIRE_AND_PURGE

    override def onProcessingTime(time: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult = TriggerResult.CONTINUE

    override def onEventTime(time: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult = TriggerResult.CONTINUE

    override def clear(window: TimeWindow, ctx: Trigger.TriggerContext): Unit = {}
  }


  //定义一个预聚合函数，去重计数
  class PreAggr extends RichAggregateFunction[LogEvent, Long, Long]{

    //定义一个状态
    var mapState: MapState[String, String] = _

    override def open(parameters: Configuration): Unit = {
      //初始化状态
      mapState = getRuntimeContext.getMapState(new MapStateDescriptor[String,String]("mapState",classOf[String],classOf[String]))
    }

    override def createAccumulator(): Long = 0L

    override def add(value: LogEvent, accumulator: Long): Long = {

      mapState.get(value.uid.toString+"|"+value.mid.toString) match {
        case value:String => {
          //已存在,就不进行计数了
          accumulator
        }
        case _ => {
          //不存在,就进行计数
          mapState.put(value.uid.toString+"|"+value.mid.toString,"1")
          accumulator+1
        }
      }

//      accumulator+1
    }

    override def getResult(accumulator: Long): Long = accumulator

    override def merge(a: Long, b: Long): Long = a+b
  }

  class ResultCount extends WindowFunction[Long,MovieCount,Long,TimeWindow]{

    override def apply(key: Long, window: TimeWindow, input: Iterable[Long], out: Collector[MovieCount]): Unit = {
      out.collect(MovieCount(key,window.getEnd,input.iterator.next()))
    }
  }

  class TopNMovies extends ProcessWindowFunction[LogEvent,Any,Long,TimeWindow]{
    override def process(key: Long, context: Context, elements: Iterable[LogEvent], out: Collector[Any]): Unit = {


      val listBuffer: ListBuffer[String] = ListBuffer()


      val iterator: Iterator[LogEvent] = elements.iterator


      while (iterator.hasNext){
        val event: LogEvent = iterator.next()

        if (!listBuffer.contains(event.uid+"|"+event.mid)){

          listBuffer += event.uid+"|"+event.mid
        }

      }

      elements.foreach(event => {
        if (!listBuffer.contains(event.uid+"|"+event.mid)){

          listBuffer += event.uid+"|"+event.mid
        }
      })

      val resultMap: Map[Long, Int] = listBuffer.map(x => {
        val strings: Array[String] = x.split("\\|")

        (strings(1).toLong, 1)

      })
        .groupBy(_._1)
        .map {
          case (key, count) => {
            var sum = 0
            count.foreach(sum += _._2)

            (key, sum)
          }
        }

      val start: Long = context.window.getStart
      val end: Long = context.window.getEnd

      val format = new SimpleDateFormat("yyyy-MM-dd-HH:mm")
      val winStart: String = format.format(new Date(start))
      val winEnd: String = format.format(new Date(end))

      print("窗口时间为：" +winStart + "到" + winEnd)

      println(",前10的电影为：======================")
      resultMap.foreach(println(_))


    }
  }

}
