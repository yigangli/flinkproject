package com.lyg.realtime.app.dwm

import com.alibaba.fastjson.serializer.SerializerFeature
import com.alibaba.fastjson.{JSON, JSONObject}
import com.lyg.realtime.bean.{OrderDetail, OrderInfo, OrderWide}
import com.lyg.realtime.common.Config
import com.lyg.realtime.utils.{DimAsyncFunction, MyKafkaUtil}
import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.api.common.functions.{MapFunction, RichMapFunction}
import org.apache.flink.api.java.functions.KeySelector
import org.apache.flink.configuration.Configuration
import org.apache.flink.runtime.state.filesystem.FsStateBackend
import org.apache.flink.streaming.api.datastream.{AsyncDataStream, KeyedStream, SingleOutputStreamOperator}
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

import java.text.SimpleDateFormat
import java.time.Duration
import java.util.concurrent.TimeUnit

/**
 * @author: yigang
 * @date: 2021/4/21
 * @desc: 订单宽表
 *
 *        业务执行流程
 *        -模拟生产数据
 *        -数据插入MySQL中
 *        -在binlog中记录数据的变化
 *        -maxwell将变化以json形式发送到ods_base_db_m中
 *        -basedbapp读取ods_base_db_m中的数据进行分流
 *        >从mysql配置表中读取数据
 *        >将配置缓存到map集合
 *        >检查phoenix中的表是否存在
 *        >对数据分流发送到不同的dwd主题
 */

object OrderWideApp {

    def main(args: Array[String]): Unit = {
        // TODO: 获取环境
        val env = StreamExecutionEnvironment.getExecutionEnvironment

        env.setParallelism(1)
        // TODO: 从kafka读取订单和订单明细数据

        env.enableCheckpointing(5000)
        env.setStateBackend(new FsStateBackend(Config.checkpoint_url+"OrderWodeApp"))
        env.getCheckpointConfig.setCheckpointTimeout(60000)

        val orderInfoSourceTopic = "dwd_order_info"
        val orderDetailSourceTopic = "dwd_order_detail"
        val orderWideSinkTopic = "dwm_order_wide"
        val groupId = "order_wide_group"

        System.setProperty("HADOOP_USER_NAME", "hdfs")
        //订单主题数据
        val orderInfoJsonStrDS = env.addSource(MyKafkaUtil.getKafkaSource(orderInfoSourceTopic, groupId))
        //订单明细数据
        val orderDetaiJsonStrDS = env.addSource(MyKafkaUtil.getKafkaSource(orderDetailSourceTopic, groupId))

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

        // TODO: 转换订单明细数据结构
        val orderDetailDS: SingleOutputStreamOperator[OrderDetail] = orderDetaiJsonStrDS.map(new RichMapFunction[String, OrderDetail] {
            var sdf: SimpleDateFormat = null

            override def open(parameters: Configuration): Unit = {
                sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
            }

            override def map(jsonStr: String): OrderDetail = {
                val orderDetail = JSON.parseObject(jsonStr, classOf[OrderDetail])
                orderDetail.setCreate_ts(sdf.parse(orderDetail.getCreate_time).getTime)
                orderDetail
            }
        })

        orderInfoDS.print("订单信息>>>>")
        orderDetailDS.print("订单明细>>>>")

        // TODO: 指定事件时间字段
        val orderInfoWithTsDS: SingleOutputStreamOperator[OrderInfo] = orderInfoDS.assignTimestampsAndWatermarks(
            WatermarkStrategy.forBoundedOutOfOrderness[OrderInfo](Duration.ofSeconds(3))
              .withTimestampAssigner(new SerializableTimestampAssigner[OrderInfo] {
                  override def extractTimestamp(orderInfo: OrderInfo, l: Long): Long = {
                      orderInfo.getCreate_ts
                  }
              })
        )

        val orderDetailWithTsDS: SingleOutputStreamOperator[OrderDetail] = orderDetailDS.assignTimestampsAndWatermarks(
            WatermarkStrategy.forBoundedOutOfOrderness[OrderDetail](Duration.ofSeconds(3))
              .withTimestampAssigner(new SerializableTimestampAssigner[OrderDetail] {
                  override def extractTimestamp(orderDetail: OrderDetail, l: Long): Long = {
                      orderDetail.getCreate_ts
                  }
              })
        )

        // TODO: 按照订单id进行分组，指定关联的key
        val orderInfoKeyedDS: KeyedStream[OrderInfo, Long] = orderInfoWithTsDS.keyBy(new KeySelector[OrderInfo, Long] {
            override def getKey(in: OrderInfo): Long = in.getId
        })

        val orderDetailKeyedDS: KeyedStream[OrderDetail, Long] = orderDetailWithTsDS.keyBy(new KeySelector[OrderDetail, Long] {
            override def getKey(in: OrderDetail): Long = in.getOrder_id
        })

        // TODO: 使用intervalJoin对订单和订单明细进行关联
        val orderWideDStream: SingleOutputStreamOperator[OrderWide] = orderInfoKeyedDS.intervalJoin(orderDetailKeyedDS)
          .between(Time.milliseconds(-5), Time.milliseconds(5))
          .process(new ProcessJoinFunction[OrderInfo, OrderDetail, OrderWide] {
              override def processElement(orderInfo: OrderInfo, orderDetail: OrderDetail, context: ProcessJoinFunction[OrderInfo, OrderDetail, OrderWide]#Context, collector: Collector[OrderWide]): Unit = {
                  collector.collect(new OrderWide(orderInfo, orderDetail))
              }
          })

        orderWideDStream.print("joined>>")
        // TODO: 关联用户维度信息
        val orderWideWithUserDstream: SingleOutputStreamOperator[OrderWide] = AsyncDataStream.unorderedWait(orderWideDStream, new DimAsyncFunction[OrderWide]("DIM_USER_INFO") {
            /**
             * 需要实现如何把结果装配给数据流对象
             *
             * @param t          数据流对象
             * @param jsonObject 异步查询结果
             * @throws java.lang.Exception
             */
            override def join(orderWide: OrderWide, jsonObject: JSONObject): Unit = {
                val formattor = new SimpleDateFormat("yyyy-MM-dd")
                val birthday = jsonObject.getString("BIRTHDAY")
                val date = formattor.parse(birthday)

                val currTs = System.currentTimeMillis()
                val betweenMs = currTs - date.getTime
                val ageLong = betweenMs / 1000L / 60L / 60L / 24L / 365L
                val age = ageLong.intValue()
                orderWide.setUser_age(age)
                orderWide.setUser_gender(jsonObject.getString("GENDER"))

            }

            /**
             * 需要实现如何从流中对象获取主键
             *
             * @param t 数据流对象
             */
            override def getKey(orderWide: OrderWide): String = {
                orderWide.getUser_id.toString
            }
        }, 60, TimeUnit.SECONDS)

        //TODO: 关联省市维度信息
        val orderWideWithProvinceDstream = AsyncDataStream.unorderedWait(
            orderWideWithUserDstream,new DimAsyncFunction[OrderWide]("DIM_BASE_PROVINCE") {
                /**
                 * 需要实现如何把结果装配给数据流对象
                 *
                 * @param t          数据流对象
                 * @param jsonObject 异步查询结果
                 * @throws java.lang.Exception
                 */
                override def join(orderWide: OrderWide, jsonObject: JSONObject): Unit = {
                    orderWide.setProvince_name(jsonObject.getString("NAME"))
                    orderWide.setProvince_3166_2_code(jsonObject.getString("ISO_3166_2"))
                    orderWide.setProvince_iso_code(jsonObject.getString("ISO_CODE"))
                    orderWide.setProvince_area_code(jsonObject.getString("AREA_CODE"))
                }

                /**
                 * 需要实现如何从流中对象获取主键
                 *
                 * @param t 数据流对象
                 */
                override def getKey(orderWide: OrderWide): String = {
                    orderWide.getProvince_id.toString
                }

            },60,TimeUnit.SECONDS
        )

        // TODO: 关联SKU维度信息
        val orderWidewWithSkuDstream = AsyncDataStream.unorderedWait(orderWideWithProvinceDstream,new DimAsyncFunction[OrderWide]("DIM_SKU_INFO") {
            /**
             * 需要实现如何把结果装配给数据流对象
             *
             * @param t          数据流对象
             * @param jsonObject 异步查询结果
             * @throws java.lang.Exception
             */
            override def join(orderWide: OrderWide, jsonObject: JSONObject): Unit = {
                orderWide.setSku_name(jsonObject.getString("SKU_NAME"))
                orderWide.setCategory3_id(jsonObject.getLong("CATEGORY3_ID"))
                orderWide.setSpu_id(jsonObject.getLong("SPU_ID"))
                orderWide.setTm_id(jsonObject.getLong("TM_ID"))
            }

            /**
             * 需要实现如何从流中对象获取主键
             *
             * @param t 数据流对象
             */
            override def getKey(orderWide: OrderWide): String = {
                orderWide.getSku_id.toString
            }
        },60,TimeUnit.SECONDS)

        // TODO: 关联spu维度信息
        val orderWideWithSpuDstream = AsyncDataStream.unorderedWait(orderWidewWithSkuDstream,new DimAsyncFunction[OrderWide]("DIM_SPU_INFO") {
            /**
             * 需要实现如何把结果装配给数据流对象
             *
             * @param t          数据流对象
             * @param jsonObject 异步查询结果
             * @throws java.lang.Exception
             */
            override def join(orderWide: OrderWide, jsonObject: JSONObject): Unit = {
                orderWide.setSpu_name(jsonObject.getString("SPU_NAME"))
            }

            /**
             * 需要实现如何从流中对象获取主键
             *
             * @param t 数据流对象
             */
            override def getKey(orderWide: OrderWide): String = {
                orderWide.getSpu_id.toString
            }
        },60,TimeUnit.SECONDS)
        val orderWideWithCategory3Dstream: SingleOutputStreamOperator[OrderWide] = AsyncDataStream.unorderedWait(orderWideWithSpuDstream, new DimAsyncFunction[OrderWide]("DIM_BASE_CATEGORY3") {
            /**
             * 需要实现如何把结果装配给数据流对象
             *
             * @param t          数据流对象
             * @param jsonObject 异步查询结果
             * @throws java.lang.Exception
             */
            override def join(orderWide: OrderWide, jsonObject: JSONObject): Unit = {
                orderWide.setCategory3_name(jsonObject.getString("NAME"))
            }

            /**
             * 需要实现如何从流中对象获取主键
             *
             * @param t 数据流对象
             */
            override def getKey(orderWide: OrderWide): String = {
                orderWide.getCategory3_id.toString
            }
        }, 60, TimeUnit.SECONDS)
        val orderWideWithTMDstream = AsyncDataStream.unorderedWait(orderWideWithCategory3Dstream,new DimAsyncFunction[OrderWide]("DIM_BASE_TRADEMARK") {
            /**
             * 需要实现如何把结果装配给数据流对象
             *
             * @param t          数据流对象
             * @param jsonObject 异步查询结果
             * @throws java.lang.Exception
             */
            override def join(orderWide: OrderWide, jsonObject: JSONObject): Unit = {
                orderWide.setTm_name(jsonObject.getString("TM_NAME"))
            }

            /**
             * 需要实现如何从流中对象获取主键
             *
             * @param t 数据流对象
             */
            override def getKey(orderWide: OrderWide): String = {
                orderWide.getTm_id.toString
            }
        },60,TimeUnit.SECONDS)
        orderWideWithTMDstream.print("dim join:")
        orderWideWithTMDstream.map(new MapFunction[OrderWide,String] {
            override def map(orderWide: OrderWide): String = {
                JSON.toJSONString(orderWide,SerializerFeature.QuoteFieldNames)
            }
        }).addSink(MyKafkaUtil.getKafkaSink(orderWideSinkTopic))
        env.execute()

    }
}
