package com.lyg.realtime.app.dwm

import com.alibaba.fastjson.{JSON, JSONObject}
import com.lyg.realtime.utils.MyKafkaUtil
import org.apache.flink.api.common.functions.{MapFunction, RichFilterFunction}
import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.api.common.state.{StateTtlConfig, ValueState, ValueStateDescriptor}
import org.apache.flink.api.common.time.Time
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.configuration.Configuration
import org.apache.flink.runtime.state.filesystem.FsStateBackend
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer

import java.text.SimpleDateFormat

/**
 * @author: yigang
 * @date: 2021/4/8
 * @desc:
 */
object UniqueView {

  def main(args: Array[String]): Unit = {
    val conf = new Configuration()
    conf.setString("rest.port","8881")
    val env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf)
    env.setParallelism(1)
    env.enableCheckpointing(5000,CheckpointingMode.EXACTLY_ONCE)
    env.getCheckpointConfig.setCheckpointTimeout(60000)
    env.setStateBackend(new FsStateBackend("hdfs://cdh-node1:8020/home/flink/checkpoint/UniqueView"))

    env.setRestartStrategy(RestartStrategies.noRestart())
    System.setProperty("HADOOP_USER_NAME", "hdfs")

    val topic = "dwd_page_log"
    val kafkaConsumer: FlinkKafkaConsumer[String] = MyKafkaUtil.getKafkaSource(topic, "uv_app")
    val kafkaDS = env.addSource(kafkaConsumer)
    kafkaDS.print()

    val jsonDS: DataStream[JSONObject] = kafkaDS.map(new MapFunction[String, JSONObject] {
      override def map(str: String) = {
        val json = JSON.parseObject(str)
        json
      }
    })
    val keyedDS = jsonDS.keyBy(json=>{
      json.getJSONObject("common").getString("mid")
    })

    val filterDS = keyedDS.filter(new RichFilterFunction[JSONObject] {
      var lastVisitDateState:ValueState[String] = null
      var sdf:SimpleDateFormat = null

      override def open(parameters: Configuration): Unit = {
        sdf = new SimpleDateFormat("yyyy-MM-dd")
        if(lastVisitDateState == null){
          val lastVisitDateStateDesc = new ValueStateDescriptor[String]("lastViewDateState",classOf[String])
          val stateTtlConfig = StateTtlConfig.newBuilder(Time.days(1)).build()

          lastVisitDateStateDesc.enableTimeToLive(stateTtlConfig)
          lastVisitDateState = getRuntimeContext.getState(lastVisitDateStateDesc)

        }

      }
      override def filter(json: JSONObject): Boolean = {
        val last_page_id = json.getJSONObject("page").getString("last_page_id")
        if(last_page_id!=null &&last_page_id.nonEmpty){
          return false
        }
        val ts = json.getLong("ts")
        val logdate = sdf.format(ts)
        val lastVisitDate = lastVisitDateState.value()

        if(lastVisitDate!=null&&lastVisitDate.nonEmpty&&lastVisitDate.equals(logdate)){
          false
        }else{
          lastVisitDateState.update(logdate)
          true
        }
      }
    }).uid("uvFilter")

    filterDS.print("filter>>>")
    val kafkaProducer = MyKafkaUtil.getKafkaSink("dwm_unique_visit")
    filterDS.map(jsonObj => jsonObj.toJSONString)
      .addSink(kafkaProducer)

    env.execute("UniqueView")
  }
}
