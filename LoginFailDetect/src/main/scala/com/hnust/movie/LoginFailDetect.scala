package com.hnust.movie

import java.io.FileInputStream
import java.sql.{Connection, PreparedStatement}
import java.util
import java.util.Properties


import com.hnust.movie.entity.CaseClasses.LoginEvent
import com.hnust.movie.utils.DBUtil
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.cep.PatternSelectFunction
import org.apache.flink.cep.scala.{CEP, PatternStream}
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.kafka.clients.consumer.ConsumerConfig

//警告信息样例类
case class LoginWarn(uid:Long,time:String, message:String)

/**
  * @Title:用户恶意登录监控，Flink实现
  * 日志格式：uid|ip|eventType|timestamp,(用户id|登录ip|事件类型，success或fail|时间戳)
  * @Author: ggh
  * @Date: 2020/6/19 9:32
  */
object LoginFailDetect {

  def main(args: Array[String]): Unit = {


    //获取执行环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    env.setParallelism(1)

    //设置时间语义为事件时间
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    //kafka配置
    val prop = new Properties()

    val propertiesPath: String = getClass().getResource("/application.properties").getPath

    prop.load(new FileInputStream(propertiesPath))

    //从配置文件获取kafka的地址
    val kafkaServers: String = prop.getProperty("kafka.broker.servers")

    prop.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,kafkaServers)
    prop.put(ConsumerConfig.GROUP_ID_CONFIG,"login-group")
    prop.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringDeserializer")
    prop.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringDeserializer")
    prop.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"latest")

    //创建source
    val kafkaSource: FlinkKafkaConsumer[String] = new FlinkKafkaConsumer[String]("login",new SimpleStringSchema(),prop)
    
    val kafkaStream: DataStream[String] = env.addSource(kafkaSource)

    kafkaStream.print("login date:")

    val keyedStream: KeyedStream[LoginEvent, Long] = kafkaStream.map(log => {
      val strings: Array[String] = log.toString.split("\\|")

      LoginEvent(strings(0).toLong, strings(1), strings(2), strings(3).toLong)
    })
      //设置延迟时间和提取时间戳
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[LoginEvent](Time.seconds(0)) {
      override def extractTimestamp(element: LoginEvent) = {
        element.timestamp
      }
    })
      .keyBy(_.uid)

    //设置匹配模式
    val pattern: Pattern[LoginEvent, LoginEvent] = Pattern.begin[LoginEvent]("begin")
      .where(_.eventType.equals("fail")) //设置以fail类型开始
      .next("next")
      .where(_.eventType.equals("fail")) //设置fail之后还是fail类型
      .within(Time.seconds(2)) //设置时间在2s范围内

    //结合stream流进行事件匹配
    val patternStream: PatternStream[LoginEvent] = CEP.pattern(keyedStream,pattern)

    //提取出符合条件的事件并进行处理
    patternStream.select(new LoginFailSelectFunction).print()
    env.execute("开始对用户登录进行监控......")

  }


  //自定义事件选择器
  class LoginFailSelectFunction() extends PatternSelectFunction[LoginEvent,LoginWarn]{


    //提取事件
    override def select(pattern: util.Map[String, util.List[LoginEvent]]): LoginWarn = {

      //提取出事件中的一个时间戳
      val timestamp: Long = pattern.get("begin").iterator().next().timestamp
      val endTime = timestamp + 1000*60*30

      //获取用户id
      val uid: Long = pattern.get("next").iterator().next().uid

      //获取数据库连接
      val connection: Connection = DBUtil.getConnection()

      val sql = "UPDATE m_userinfo SET no_login_time=? WHERE uid=?"
      val ps: PreparedStatement = connection.prepareStatement(sql)

      ps.setString(1,endTime.toString)
      ps.setLong(2,uid)

      val result: Int = ps.executeUpdate()

      if (result > 0){
        println("已限制用户 "+uid + " 在半小时后才可登录:"+endTime)
      }

      //关闭连接
      DBUtil.close(connection)

      LoginWarn(pattern.get("begin").iterator().next().uid, timestamp.toString,
        "该用户在短时间内连续登录失败，怀疑有恶意登录")

    }
  }

}
