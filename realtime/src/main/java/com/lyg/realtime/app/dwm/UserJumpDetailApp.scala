package com.lyg.realtime.app.dwm

import com.alibaba.fastjson.{JSON, JSONObject}
import com.lyg.realtime.common.Config
import com.lyg.realtime.utils.MyKafkaUtil
import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.api.java.functions.KeySelector
import org.apache.flink.cep.{CEP, PatternFlatSelectFunction, PatternFlatTimeoutFunction}
import org.apache.flink.cep.pattern.Pattern
import org.apache.flink.cep.pattern.conditions.SimpleCondition
import org.apache.flink.runtime.state.filesystem.FsStateBackend
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.datastream._
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala.{OutputTag, createTypeInformation}
import org.apache.flink.streaming.api.windowing.assigners.{SlidingEventTimeWindows, TumblingEventTimeWindows}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.runtime.operators.TimestampsAndWatermarksOperator.WatermarkEmitter
import org.apache.flink.util.Collector

import java.util

/**
 * @author: yigang
 * @date: 2021/4/13
 * @desc:
 */
object UserJumpDetailApp {

  def main(args: Array[String]): Unit = {

    // TODO: 获取flink环境 
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    env.enableCheckpointing(5000,CheckpointingMode.EXACTLY_ONCE)
    env.setStateBackend(new FsStateBackend(Config.checkpoint_url+"UserJumpDetailApp"))
    env.getCheckpointConfig.setCheckpointTimeout(60000)
    env.setRestartStrategy(RestartStrategies.noRestart())

    //flink1.12开始默认时间语义为事件时间，之前都是处理时间，这个地方不需要额外指定了
//    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    System.setProperty("HADOOP_USER_NAME","hdfs")

    val srcTopic = "dwd_page_log"
    val sinkTopic = "dwm_user_jump_detail"
    val kafkaSource = MyKafkaUtil.getKafkaSource(srcTopic,"UserJump")
    val kafkaDS = env.addSource(kafkaSource)
    //测试数据
//    val kafkaDS = env.fromElements("{\"common\":{\"mid\":\"101\"},\"page\":{\"page_id\":\"home\"},\"ts\":10000} ",
//      "{\"common\":{\"mid\":\"102\"},\"page\":{\"page_id\":\"home\"},\"ts\":12000}",
//      "{\"common\":{\"mid\":\"102\"},\"page\":{\"page_id\":\"good_list\",\"last_page_id\":\"home\"},\"ts\":150000} ",
//      "{\"common\":{\"mid\":\"102\"},\"page\":{\"page_id\":\"good_list\",\"last_page_id\":\"detail\"},\"ts\":300000} "
//    )
    // TODO: 对读取的数据转换成JSONObject
    val jsonObjectDS:DataStream[JSONObject] = kafkaDS.map(new MapFunction[String,JSONObject] {
      override def map(jsonStr: String): JSONObject={
        JSON.parseObject(jsonStr)
      }
    })
    jsonObjectDS.print("接收到json>>>>>")

    // TODO: 指定事件时间和水位线
    val jsonObjWithTs: DataStream[JSONObject] = jsonObjectDS.assignTimestampsAndWatermarks(WatermarkStrategy.forMonotonousTimestamps().
      withTimestampAssigner(new SerializableTimestampAssigner[JSONObject] {
        override def extractTimestamp(t: JSONObject, l: Long): Long = {
          t.getLong("ts")
        }
      }))

    // TODO: 按照设备mid分组
//    val keyedDS: KeyedStream[JSONObject, String] = jsonObjWithTs.keyBy(jsonObj => jsonObj.getJSONObject("common").getString("mid"))
      val keyedDS: KeyedStream[JSONObject, String] = jsonObjWithTs.keyBy(new KeySelector[JSONObject,String] {
        override def getKey(jsonObj: JSONObject): String = jsonObj.getJSONObject("common").getString("mid")
      })

    /*
      1.不是从其它页面跳转过来的页面,是首次访问的用户
      2.距离首次访问结束后10秒内，没有对其它的页面再进行访问
     */
    // TODO: 配置cep表达式
    //指第一条收到的数据中上次访问页面为空，并且访问的下一个页面不为空并且是在访问第一个事件之后的10秒内完成跳转的事件留下来
    val pattern = Pattern.begin[JSONObject]("first")
      .where(
        //条件1.不是从其它页面跳转过来的页面,是首次访问的用户
        new SimpleCondition[JSONObject] {
          override def filter(jsonObj: JSONObject): Boolean = {
            //获取last_page_id
            val lastPageId = jsonObj.getJSONObject("page").getString("last_page_id")
            if(lastPageId==null||lastPageId.isEmpty){
              return true
            }
            false
          }
        }
      )
      .next("next")
      .where(
        //条件2.距离首次访问结束后10秒内，没有对其它的页面再进行访问
        new SimpleCondition[JSONObject] {
          override def filter(jsonObj: JSONObject): Boolean = {
            val page_id = jsonObj.getJSONObject("page").getString("page_id")
            if(page_id!=null&&page_id.nonEmpty){
              return true
            }
            false
          }
        }
      )
      .within(Time.milliseconds(10000))
    val patternedStream = CEP.pattern(keyedDS, pattern)
    val timeoutTag = new OutputTag[String]("timeout")
    val filteredStream: SingleOutputStreamOperator[String] = patternedStream.flatSelect(timeoutTag, new PatternFlatTimeoutFunction[JSONObject, String] {
      override def timeout(pattern: util.Map[String, util.List[JSONObject]], l: Long, collector: Collector[String]): Unit = {
        val jsonIter = pattern.get("first").iterator()
        while (jsonIter.hasNext) {
          collector.collect(jsonIter.next().toJSONString)
        }
      }
    }
      ,
      new PatternFlatSelectFunction[JSONObject, String] {
        override def flatSelect(map: util.Map[String, util.List[JSONObject]], collector: Collector[String]): Unit = {

        }
      })
    //通过侧输出流提取超时数据
    val jumpDS = filteredStream.getSideOutput(timeoutTag)
    jumpDS.print("跳出数据>>>>>>")
    jumpDS.addSink(MyKafkaUtil.getKafkaSink(sinkTopic))

    env.execute("UserJumpDetailApp")
  }
}

