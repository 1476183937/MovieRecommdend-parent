package com.hnust.movie

import java.io.FileInputStream
import java.lang
import java.text.SimpleDateFormat
import java.util.{Date, Properties}

import com.hnust.movie.utils.DateUtil
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.triggers.{Trigger, TriggerResult}
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.util.Collector
import org.apache.kafka.clients.consumer.ConsumerConfig
import redis.clients.jedis.Jedis


case class LogEvent(uid: Long, ip: String,mid:Long, event: String, timestamp: Long)

//输出结果样例类:开始时间-结束时间-数量
case class UVCount(startTime:String, endTime:String, count:Long)

/**
  * @Title:统计每一天当中每小时内的用户访问量
  * 用户行为日志格式：uid|ip|mid|event|timestamp,电影id（mid）是可能为空的
  * event的值有:detail(浏览详情页)、play(进入播放页)、fav(收藏操作)、rating(评分操作)、
  * search(搜索操作)、index(进入首页)、comment(评论操作)、login(登录操作)、
  * @Author: ggh
  * @Date: 2020/6/19 11:49
  */
object UVCount {

  def main(args: Array[String]): Unit = {

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    //定义kafka的配置
    val prop = new Properties()
    val path: String = getClass.getResource("/application.properties").getPath

    prop.load(new FileInputStream(path))

    val kafkaBrokers: String = prop.getProperty("kafka.broker.servers")

    prop.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBrokers)
    prop.put(ConsumerConfig.GROUP_ID_CONFIG, "event")
    prop.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer")
    prop.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer")
    prop.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest")

    val kafkaConsumer: FlinkKafkaConsumer[String] = new FlinkKafkaConsumer[String]("event", new SimpleStringSchema(), prop)

    val kafkaStream: DataStream[String] = env.addSource(kafkaConsumer)

    //    用户行为日志格式：uid|ip|mid|event|timestamp，电影id是可能为空的
    //    event的值有:detail(浏览详情页)、play(进入播放页)、fav(收藏操作)、rating(评分操作)、search(搜索操作)、index(进入首页)、comment(评论操作)、login(登录操作)、
    kafkaStream.print("event data:")

    //定义一个侧输出流
    val outPutTag = new OutputTag[(String, Long, String, Long)]("late date:")

    val resultStream: DataStream[UVCount] = kafkaStream
      .filter(_.split("\\|").length == 4) //过滤掉不符合要求的日志
      .map(line => {
      val strings: Array[String] = line.split("\\|")

      LogEvent(strings(0).toLong, strings(1),strings(2).toLong, strings(3), strings(4).toLong)
    })
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[LogEvent](Time.minutes(1)) {
        override def extractTimestamp(element: LogEvent) = {
          element.timestamp
        }
      })
      .map(event => ("tmpKey", event.uid, event.ip, event.timestamp))
      .keyBy(_._1)
      .timeWindow(Time.hours(1))
      .allowedLateness(Time.minutes(1))  //允许数据迟到一分钟
      .sideOutputLateData(outPutTag)               //将迟到的数据放到侧输出流
      .trigger(new EventTrigger)
      .process(new UVCountWithBloom())

    //接收一下迟到的数据
    resultStream.getSideOutput(outPutTag).print("迟到的数据：")

    //输出一下实时统计结果
    resultStream.print("result count:")

    env.execute("开始统计uv：")

  }

  //自定义触发器，来一条数据就触发一次计算
  class EventTrigger extends Trigger[(String, Long, String, Long), TimeWindow] {
    override def onElement(element: (String, Long, String, Long), timestamp: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult = TriggerResult.FIRE_AND_PURGE

    override def onProcessingTime(time: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult = TriggerResult.CONTINUE

    override def onEventTime(time: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult = {

//      ctx.registerEventTimeTimer(1)

      TriggerResult.CONTINUE
    }

    override def clear(window: TimeWindow, ctx: Trigger.TriggerContext): Unit = {
    }
  }


  /**
    * @title:
    * @description: 自定义布隆过滤器
    * @param: size:过滤器大小
    * @author:ggh
    * @updateTime: 2020/6/19 20:08
    **/
  class Bloom(size: Int) extends Serializable {

    var crap = if (size > 0) size else 1 << 27

    //hash函数，根据传入的string求对应的偏移量
    def hash(value: String, seed: Int): Long = {

      var result = 0L
      for (i <- 0 until value.length) {
        result = result * seed + value.charAt(i)
      }

      result & crap - 1
    }

  }


  class UVCountWithBloom extends ProcessWindowFunction[(String, Long, String, Long), UVCount,String,TimeWindow]{

    //定义一个布隆过滤器
    lazy val bloom:Bloom = new Bloom(1<<23)

    //定义jedis连接
    lazy val jedis: Jedis = new Jedis("localhost",6379)

    override def close(): Unit = {

      //在窗口要关闭的时候将缓存中的数据写入数据库

      //关闭资源
      jedis.close()

    }


    override def process(key: String, context: Context, elements: Iterable[(String, Long, String, Long)], out: Collector[UVCount]): Unit = {

      //在redis缓存中的存储结构：一个位图,名称为每个小时的开始时间_结束时间，
      // 如2020-06-05-03_2020-06-05-04,表示2020年6月5日3时到4时

      //获取事件中的时间戳
      val timestamp: Long = elements.head._4

      val date = new Date(timestamp)
      val startTime: String = DateUtil.formatByDate(date,"yyyy-MM-dd-HH")

      val endTime: String = startTime.substring(0, startTime.length - 1)+(startTime.substring(startTime.length - 1).toInt + 1).toString

      //拼接redis中的存储key
      val storeKey = startTime + "_" + endTime

      val uid2Ip: String = elements.head._2.toString + elements.head._3.toString

      //获取在位图中的位置
      val offect: Long = bloom.hash(uid2Ip,61)

      val result: lang.Boolean = jedis.getbit(storeKey,offect)

      val count: lang.Long = jedis.bitcount(storeKey)

      val start: Long = context.window.getStart
      val end: Long = context.window.getEnd

      val format = new SimpleDateFormat("yyyy-MM-dd-HH")
      val winStart: String = format.format(new Date(start))
      val winEnd: String = format.format(new Date(end))

      println("窗口时间为：" +winStart + "到" + winEnd)

      //侧输出流
//      context.output(new OutputTag[UVCount]("late"),UVCount(startTime,endTime,11))

      //如果位图中指定位置为0，说明之前没有记录过，就将其设置为1
      if (!result){
        jedis.setbit(storeKey,offect,true)
        out.collect(UVCount(startTime,endTime,count+1))
      }else {
        out.collect(UVCount(startTime,endTime,count))
      }


    }

  }


}
