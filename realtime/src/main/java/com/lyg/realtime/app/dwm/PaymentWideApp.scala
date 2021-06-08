package com.lyg.realtime.app.dwm

import com.alibaba.fastjson.serializer.SerializerFeature
import com.alibaba.fastjson.{JSON, JSONObject}
import com.lyg.realtime.bean.{OrderWide, PaymentInfo, PaymentWide}
import com.lyg.realtime.common.Config
import com.lyg.realtime.utils.{DateTimeUtil, MyKafkaUtil}
import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.api.java.functions.KeySelector
import org.apache.flink.configuration.Configuration
import org.apache.flink.runtime.state.filesystem.FsStateBackend
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

import java.time.Duration

/**
 * @author: yigang
 * @date: 2021/6/8
 * @desc:
 */
object PaymentWideApp {

    def main(args: Array[String]): Unit = {

        // TODO: 创建表执行环境
        val conf = new Configuration()
        conf.setString("rest.port", "8855")
        val env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf)
        env.setParallelism(1)
        env.enableCheckpointing(5000,CheckpointingMode.EXACTLY_ONCE)
        env.getCheckpointConfig.setCheckpointTimeout(60000)
        env.setStateBackend(new FsStateBackend(Config.checkpoint_url+"PaymentWideApp"))

        System.setProperty("HADOOP_USER_NAME","hdfs")

        val gourpid="payment_wide_app"
        val paymentInfoSourceTopic = "dwd_payment_info"
        val orderWideSourceTopic = "dwm_order_wide"
        val paymentWideSinkTopic = "dwm_payment_wide"

        env.setRestartStrategy(RestartStrategies.noRestart)
        // TODO: 获取支付信息流和订单宽表，并解析成bean
        val paymentInfoDstream = env.addSource(MyKafkaUtil.getKafkaSource(paymentInfoSourceTopic,gourpid))
          .map(new MapFunction[String,PaymentInfo] {
              override def map(t: String): PaymentInfo = {
                  JSON.parseObject(t,classOf[PaymentInfo])
              }
          })

        val orderWideDstream = env.addSource(MyKafkaUtil.getKafkaSource(orderWideSourceTopic,gourpid))
          .map(new MapFunction[String,OrderWide] {
              override def map(t: String): OrderWide = {
                  JSON.parseObject(t,classOf[OrderWide])
              }
          })
        orderWideDstream.print("o>>")
        paymentInfoDstream.print("p>>")
        // TODO: 给两个流设置水位线和时间戳
        val paymentInfoWithWatermarkDS: SingleOutputStreamOperator[PaymentInfo] = paymentInfoDstream.assignTimestampsAndWatermarks(
            WatermarkStrategy.forBoundedOutOfOrderness[PaymentInfo](Duration.ofSeconds(3))
              .withTimestampAssigner(
                  new SerializableTimestampAssigner[PaymentInfo] {
                      override def extractTimestamp(t: PaymentInfo, l: Long): Long = {
                          DateTimeUtil.toTs(t.getCallback_time)
                      }
                  }
              )
        )
        val orderWideWithWatermarkDS: SingleOutputStreamOperator[OrderWide] = orderWideDstream.assignTimestampsAndWatermarks(
            WatermarkStrategy.forBoundedOutOfOrderness[OrderWide](Duration.ofSeconds(3))
              .withTimestampAssigner(new SerializableTimestampAssigner[OrderWide] {
                  override def extractTimestamp(t: OrderWide, l: Long): Long = {
                      DateTimeUtil.toTs(t.getCreate_time)
                  }
              })
        )
        // TODO: 为双流join做key的分区
        val paymentKeyedDstream = paymentInfoWithWatermarkDS.keyBy(new KeySelector[PaymentInfo,Long] {
            override def getKey(paymentInfo: PaymentInfo): Long = {
                paymentInfo.getOrder_id
            }
        })
        val orderWideKeyedDstream = orderWideWithWatermarkDS.keyBy(new KeySelector[OrderWide,Long] {
            override def getKey(orderWide: OrderWide): Long = {
                orderWide.getOrder_id
            }
        })
        // TODO: 双流intervalJoin，输出paymentWide流
        val paymentWideDstream = paymentKeyedDstream.intervalJoin(orderWideKeyedDstream).
          between(Time.seconds(-1800), Time.seconds(0)).
          process(new ProcessJoinFunction[PaymentInfo, OrderWide, PaymentWide] {
              override def processElement(paymentInfo: PaymentInfo, orderWide: OrderWide, context: ProcessJoinFunction[PaymentInfo, OrderWide, PaymentWide]#Context, collector: Collector[PaymentWide]): Unit = {
                  collector.collect(new PaymentWide(paymentInfo, orderWide))
              }
          }).uid("payment_wide_join")

        // TODO: 将paymentWide流转换成json格式字符串流发到kafka中
        paymentWideDstream.map(new MapFunction[PaymentWide,String] {
            override def map(paymentWide: PaymentWide): String = {
                JSON.toJSONString(paymentWide,SerializerFeature.QuoteFieldNames)
            }
        }).addSink(MyKafkaUtil.getKafkaSink(paymentWideSinkTopic))

        paymentWideDstream.print("pm>>>>>")
        env.execute()

    }
}
