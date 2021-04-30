package com.lyg.realtime.app.dwd

import com.alibaba.fastjson.{JSON, JSONObject}
import com.lyg.realtime.utils.MyKafkaUtil
import org.apache.flink.api.common.functions.{MapFunction, RichMapFunction}
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.api.scala.typeutils.Types
import org.apache.flink.configuration.Configuration
import org.apache.flink.runtime.state.filesystem.FsStateBackend
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.scala.{DataStream, KeyedStream, OutputTag, StreamExecutionEnvironment}
import org.apache.flink.util.Collector

import java.text.SimpleDateFormat
import java.util.Date

/**
 * @author: yigang
 * @date: 2021/3/31
 * @desc: 准备日志数据DWD层
 */
object BaseLogApp {

  def main(args: Array[String]): Unit = {
    val conf = new Configuration()
    conf.setString("rest.port", "8888")
    val env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf)

    //设置并行读，与kafka分区数量保持一致
    env.setParallelism(1)
    //设置checkpoint时间5秒一次，精准一次性
    env.enableCheckpointing(5000, CheckpointingMode.EXACTLY_ONCE)
    //做check如果超过60秒就会被抛弃
    env.getCheckpointConfig.setCheckpointTimeout(60000)

    //状态checkpoint保存点
    env.setStateBackend(new FsStateBackend("hdfs://cdh-node1:8020/home/flink/checkpoint/baselogapp"))
    System.setProperty("HADOOP_USER_NAME", "hdfs")

    val topic = "ods_base_log"
    val group_id = "ods_dwd_base_log_app"

    // TODO: 1.从kafka中读取数据
    val flinkkafkaconsumer = MyKafkaUtil.getKafkaSource(topic, group_id)
    val kafkaDS = env.addSource(flinkkafkaconsumer)
    val jsonObject = kafkaDS.map(new MapFunction[String, JSONObject] {
      override def map(t: String): JSONObject = {
        JSON.parseObject(t)
      }
    })
    //    jsonObject.print()


    // TODO: 2.识别新老访客
    //mid分组
    val midKeyedDS: KeyedStream[JSONObject, String] = jsonObject.keyBy(data => {
      data.getJSONObject("common").getString("mid")
    })
    //校验采集到的数据是新老访客
    val midWithNewFlagDS = midKeyedDS.map(
      new RichMapFunction[JSONObject, JSONObject] {
        //第一次访问的日期的状态
        var firstvalueState: ValueState[String] = _
        //声明日期数据格式化对象
        var simpleDateFormat: SimpleDateFormat = _

        override def open(parameters: Configuration): Unit = {
          firstvalueState = getRuntimeContext.getState(
            new ValueStateDescriptor("newMidDateState", Types.STRING)
          )
          simpleDateFormat = new SimpleDateFormat("yyyyMMdd")
        }

        override def map(jsonObj: JSONObject): JSONObject = {
          //        println(jsonObj)
          var is_new = jsonObj.getJSONObject("common").getString("is_new")
          val ts = jsonObj.getLong("ts")
          //如果is_new值为1，进入校验
          if ("1".equals(is_new)) {
            //获取第一次访问的时间状态
            val newMidDate = firstvalueState.value()
            //格式化该次访问时间
            val tsDate = simpleDateFormat.format(new Date(ts))
            //如果第一次访问不为null且第一次访问与本次访问时间不同，则将is_new 置为0，即不是第一次访问
            if (newMidDate != null && newMidDate.nonEmpty) { //自己给自己挖坑了，newMidDate.nonEmpty && newMidDate != null写反导致出现null值后程序就停止了，应该先判断null值再判断字符串长度为0
              if (!newMidDate.equals(tsDate)) {
                is_new = "0"
                jsonObj.getJSONObject("common").put("is_new", is_new)
              }
            } else {
              //第一次访问时间为null,则将此次访问时间赋值
              firstvalueState.update(tsDate)
            }
          }
          jsonObj
        }
      })

    //midWithNewFlagDS.print()
    // TODO: 3.分流

    val startTag = new OutputTag[String]("start")
    val displaysTag = new OutputTag[String]("displays")
    val pageDS: DataStream[String] = midWithNewFlagDS.process(new ProcessFunction[JSONObject, String] {

      override def processElement(jsonObj: JSONObject, ctx: ProcessFunction[JSONObject, String]#Context, collector: Collector[String]): Unit = {
        val startJson = jsonObj.getJSONObject("start")
        val jsonStr = jsonObj.toString
        if (startJson != null && startJson.size() > 0) {
          ctx.output(startTag, jsonStr)
        } else {
          val displaysArrayJson = jsonObj.getJSONArray("displays")
          val pageid = jsonObj.getJSONObject("page").getString("page_id")
          if (displaysArrayJson != null && displaysArrayJson.size() > 0) {
            //如果是曝光日志，遍历数组输出每一条
            for (index <- 0 until displaysArrayJson.size) {
              val displayJson = displaysArrayJson.getJSONObject(index)
              displayJson.put("page_id", pageid)
              ctx.output(displaysTag, displayJson.toString)
            }
          } else {
            collector.collect(jsonStr)
          }
        }

      }
    })
    //获取侧输出流
    val startDS = pageDS.getSideOutput(startTag)
    val displaysDS = pageDS.getSideOutput(displaysTag)

    //打印输出
    //    pageDS.print("page>>>>>>>>>")
    //    startDS.print("start>>>>>>>>>")
    //    displaysDS.print("displays>>>>>>>>>")

    // TODO: 4.将分流写入kafka
    val startSink = MyKafkaUtil.getKafkaSink("dwd_start_log")
    val displaySink = MyKafkaUtil.getKafkaSink("dwd_display_log")
    val pageSink = MyKafkaUtil.getKafkaSink("dwd_page_log")

    pageDS.addSink(pageSink)
    startDS.addSink(startSink)
    displaysDS.addSink(displaySink)

    env.execute("BaseLogApp")
  }
}
