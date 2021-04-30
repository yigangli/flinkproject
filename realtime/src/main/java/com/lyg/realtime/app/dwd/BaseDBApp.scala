package com.lyg.realtime.app.dwd

import com.alibaba.fastjson.{JSON, JSONObject}
import com.lyg.realtime.app.func.{DimSink, TableProcessFunction}
import com.lyg.realtime.bean.TableProcess
import com.lyg.realtime.utils.MyKafkaUtil
import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.api.common.serialization.SerializationSchema
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.configuration.Configuration
import org.apache.flink.runtime.state.filesystem.FsStateBackend
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.scala.{DataStream, OutputTag, StreamExecutionEnvironment}
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema
import org.apache.kafka.clients.producer.ProducerRecord

import java.lang

/**
 * @author: yigang
 * @date: 2021/3/31
 * @desc: 准备业务数据DWD层
 */
object BaseDBApp {
  def main(args: Array[String]): Unit = {
    //准备环境
    val conf = new Configuration()
    conf.setString("rest.port", "8888")
    val env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf)
    //设置并行度，和kafka分区数保持一致
    env.setParallelism(1)
    //开启checkpoint
    env.enableCheckpointing(5000, CheckpointingMode.EXACTLY_ONCE)
    env.getCheckpointConfig.setCheckpointTimeout(60000)
    env.setStateBackend(new FsStateBackend("hdfs://cdh-node1:8020/home/flink/checkpoint/basedbapp"))

    //重启策略
    //如果没有开启checkpoint，默认noRestart
    //如果开启了checkpoint，flink会自动帮你重启integer。maxValue次
    env.setRestartStrategy(RestartStrategies.noRestart())
    // TODO: 1.从kafkaods层读取数据
    val topic = "ods_base_db_m"
    val kafkaConsumer = MyKafkaUtil.getKafkaSource(topic, "ods_base_db_m_app")

    val ods_base_db_DS = env.addSource(kafkaConsumer)
    //    ods_base_db_DS.print("json>>>>")

    val jsonObj: DataStream[JSONObject] = ods_base_db_DS.map(jsonStr => {
      JSON.parseObject(jsonStr)
    })

    val filterDS: DataStream[JSONObject] = jsonObj.filter(jsonObj => {
      val table = jsonObj.getString("table")
      val data = jsonObj.getJSONObject("data")
      val datalen = jsonObj.getString("data").length
      val flag = table != null && data != null && datalen >= 3
      flag
    })
    filterDS.print()

    //    // TODO: 2动态分流   事实表主流输出到kafka DWD  维度表侧输出流输出到hbases
    val hbaseTag = new OutputTag[JSONObject](TableProcess.SINK_TYPE_HBASE)
    val kafkaDS: DataStream[JSONObject] = {
      filterDS.process(new TableProcessFunction(hbaseTag))
    }
    val hbaseDS: DataStream[JSONObject] = kafkaDS.getSideOutput(hbaseTag)
    kafkaDS.print("事实<<<<<<<")
    hbaseDS.print("维度<<<<<<<")

    // TODO:  将维度数据写入hbase
    hbaseDS.addSink(new DimSink)

    // TODO:  将事实数据写入kafka
    val kafkaSink = MyKafkaUtil.getKafkaSinkBySchema[JSONObject](
      new KafkaSerializationSchema[JSONObject] {
        override def open(context: SerializationSchema.InitializationContext): Unit = {
          println("kafka序列化")
        }

        override def serialize(jsonObj: JSONObject, aLong: lang.Long): ProducerRecord[Array[Byte], Array[Byte]] = {
          val sinkTopic = jsonObj.getString("sink_table")
          val jsonData = jsonObj.getJSONObject("data")
          new ProducerRecord(sinkTopic, jsonData.toString().getBytes())
        }
      }
    )
    kafkaDS.addSink(kafkaSink)

    env.execute("BaseDBApp")
  }

}
