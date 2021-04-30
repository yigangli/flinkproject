package com.lyg.realtime.app.dwm

import com.alibaba.fastjson.JSON
import com.lyg.realtime.bean.OrderInfo
import com.lyg.realtime.utils.MyKafkaUtil
import org.apache.flink.api.common.functions.{MapFunction, RichMapFunction}
import org.apache.flink.configuration.Configuration
import org.apache.flink.runtime.state.filesystem.FsStateBackend
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment

import java.text.SimpleDateFormat

/**
 * @author: yigang
 * @date: 2021/4/21
 * @desc:
 */
object OrderWideApp {

  def main(args: Array[String]): Unit = {
    // TODO: 获取环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    env.setParallelism(1)
    // TODO: 从kafka读取订单和订单明细数据

    env.enableCheckpointing(5000)
    env.setStateBackend(new FsStateBackend("hdfs://cdh-node1:8020/home/flink/checkpoint/OrderWodeApp"))
    env.getCheckpointConfig.setCheckpointTimeout(60000)

    val orderInfoSourceTopic = "dwd_order_info"
    val orderDetailSourceTopic = "dwd_order_detail"
    val orderWideSinkTopic = "dwm_order_wide"
    val groupId = "order_wide_group"

    System.setProperty("HADOOP_USER_NAME","hdfs")
    //订单主题数据
    val orderInfoJsonStrDS = env.addSource(MyKafkaUtil.getKafkaSource(orderInfoSourceTopic,groupId))
    //订单明细数据
    val orderDetailDS = env.addSource(MyKafkaUtil.getKafkaSource(orderDetailSourceTopic,groupId))

    // TODO: 对数据进行结构转换
    val orderInfoDS: SingleOutputStreamOperator[OrderInfo] = orderInfoJsonStrDS.map(new RichMapFunction[String, OrderInfo] {
      var sdf: SimpleDateFormat = null

      override def open(parameters: Configuration): Unit = {
        sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
      }
      override def map(t: String): OrderInfo = {
        val orderInfo = JSON.parseObject(t, classOf[OrderInfo])
        orderInfo.setCreate_ts(sdf.parse(orderInfo.getCreate_time).getTime)
        orderInfo
      }
    })

  }
}
