package com.lyg.realtime.app.dws

import com.alibaba.fastjson.JSON
import com.lyg.realtime.bean.VisitorStats
import com.lyg.realtime.common.Config
import com.lyg.realtime.utils.MyKafkaUtil
import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.api.common.functions.{MapFunction, ReduceFunction}
import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.api.java.functions.KeySelector
import org.apache.flink.runtime.state.filesystem.FsStateBackend
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.datastream.{DataStream, KeyedStream, SingleOutputStreamOperator}
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import java.lang
import java.text.SimpleDateFormat
import java.time.Duration
import java.util.Date

/**
 * @author: yigang
 * @date: 2021/6/9
 * @desc: ➢ 接收各个明细数据，变为数据流
 *➢ 把数据流合并在一起，成为一个相同格式对象的数据流
 *➢ 对合并的流进行聚合，聚合的时间窗口决定了数据的时效性
 *➢ 把聚合结果写在数据库中

 */
object VisitorStatsApp {
    def main(args: Array[String]): Unit = {
        // TODO: 获取执行环境
        val env = StreamExecutionEnvironment.getExecutionEnvironment
        env.setParallelism(1)

        env.getCheckpointConfig.setCheckpointTimeout(60000)
        env.enableCheckpointing(5000,CheckpointingMode.EXACTLY_ONCE)
        env.setStateBackend(new FsStateBackend(Config.checkpoint_url+"VisitorStatsApp"))

        env.setRestartStrategy(RestartStrategies.noRestart())

        System.setProperty("HADOOP_USER_NAME","hdfs")

        val uniqueVisitSourceTopic = "dwm_unique_visit"
        val userJumpDetailSourceTopic = "dwm_user_jump_detail"
        val pageViewSourceTopic = "dwd_page_log"
        val groupid = "visitor_stats_app"
        val visitorstats_sink_topic = "dws_visit_stats"

        // TODO: dwd获取页面访问输入流、dwm获取跳出流明细和独立访客明细 
        val pageViewDstream = env.addSource(MyKafkaUtil.getKafkaSource(pageViewSourceTopic,groupid))
        val userJumpDetailDstream = env.addSource(MyKafkaUtil.getKafkaSource(userJumpDetailSourceTopic,groupid))
        val uniqueVisitDstream = env.addSource(MyKafkaUtil.getKafkaSource(uniqueVisitSourceTopic,groupid))

        pageViewDstream.print("page_view>>>")
        userJumpDetailDstream.print("user_jump>>>")
        uniqueVisitDstream.print("unique_visit>>>")

        val pageViewStatsDstream: SingleOutputStreamOperator[VisitorStats] = pageViewDstream.map(new MapFunction[String, VisitorStats] {
            override def map(jsonStr: String): VisitorStats = {
                val jsonObject = JSON.parseObject(jsonStr)
                new VisitorStats("", "",
                    jsonObject.getJSONObject("common").getString("vc"),
                    jsonObject.getJSONObject("common").getString("ch"),
                    jsonObject.getJSONObject("common").getString("ar"),
                    jsonObject.getJSONObject("common").getString("is_new"),
                    0L, 1L, 0L, 0L, jsonObject.getJSONObject("page").getLong("during_time"),
                    jsonObject.getLong("ts")
                )
            }
        })
        val uniqueVisitStatsDstream: SingleOutputStreamOperator[VisitorStats] = uniqueVisitDstream.map(new MapFunction[String, VisitorStats] {
            override def map(jsonStr: String): VisitorStats = {
                val jsonObject = JSON.parseObject(jsonStr)
                new VisitorStats("", "",
                    jsonObject.getJSONObject("common").getString("vc"),
                    jsonObject.getJSONObject("common").getString("ch"),
                    jsonObject.getJSONObject("common").getString("ar"),
                    jsonObject.getJSONObject("common").getString("is_new"),
                    1L, 0L, 0L, 0L, 0L,
                    jsonObject.getLong("ts")
                )
            }
        })
        val userJumpStatDstream: SingleOutputStreamOperator[VisitorStats] = userJumpDetailDstream.map(new MapFunction[String, VisitorStats] {
            override def map(jsonStr: String): VisitorStats = {
                val jsonObject = JSON.parseObject(jsonStr)
                new VisitorStats("", "",
                    jsonObject.getJSONObject("common").getString("vc"),
                    jsonObject.getJSONObject("common").getString("ch"),
                    jsonObject.getJSONObject("common").getString("ar"),
                    jsonObject.getJSONObject("common").getString("is_new"),
                    0L, 0L, 0L, 1L, 0L,
                    jsonObject.getLong("ts")
                )
            }
        })
        val sessionVisitDstream: SingleOutputStreamOperator[VisitorStats] = pageViewDstream.process(new ProcessFunction[String, VisitorStats] {
            override def processElement(jsonStr: String, ctx: ProcessFunction[String, VisitorStats]#Context, out: Collector[VisitorStats]): Unit = {
                val jsonObject = JSON.parseObject(jsonStr)
                val last_page_id = jsonObject.getJSONObject("page").getString("last_page_id")
                if (last_page_id == null || last_page_id.isEmpty) {
                    val record: VisitorStats = new VisitorStats("", "",
                        jsonObject.getJSONObject("common").getString("vc"),
                        jsonObject.getJSONObject("common").getString("ch"),
                        jsonObject.getJSONObject("common").getString("ar"),
                        jsonObject.getJSONObject("common").getString("is_new"),
                        0L, 0L, 1L, 0L, 0L,
                        jsonObject.getLong("ts")
                    )
                    out.collect(record)
                }

            }
        })
        val unionDetailDstream: DataStream[VisitorStats] = pageViewStatsDstream.union(uniqueVisitStatsDstream,
            userJumpStatDstream,
            sessionVisitDstream
        )

        val visitorStatsWithWatermarkDstream: SingleOutputStreamOperator[VisitorStats] = unionDetailDstream.assignTimestampsAndWatermarks(
            WatermarkStrategy.forBoundedOutOfOrderness[VisitorStats](Duration.ofSeconds(2)).
              withTimestampAssigner(new SerializableTimestampAssigner[VisitorStats] {
                  override def extractTimestamp(visitorStats: VisitorStats, recordTimestamp: Long): Long = {
                      visitorStats.getTs
                  }
              })
        )
        visitorStatsWithWatermarkDstream.print("vs>>>")

        // TODO:开窗&聚合
        val visitorStatsKeyedStream: KeyedStream[VisitorStats, (String, String, String, String)] = visitorStatsWithWatermarkDstream.keyBy(new KeySelector[VisitorStats, (String, String, String, String)] {
            override def getKey(visitorStats: VisitorStats): (String, String, String, String) = {
                (visitorStats.getVc, visitorStats.getCh, visitorStats.getAr, visitorStats.getIs_new)
            }
        })
        visitorStatsKeyedStream.window(TumblingEventTimeWindows.of(Time.seconds(10)))
          .reduce(new ReduceFunction[VisitorStats] {
              override def reduce(v1: VisitorStats, v2: VisitorStats): VisitorStats = {
                  v1.setPv_ct(v1.getPv_ct+v2.getPv_ct)
                  v1.setSv_ct(v1.getSv_ct+v2.getSv_ct)
                  v1.setUj_ct(v1.getUj_ct+v2.getUj_ct)
                  v1.setUv_ct(v1.getUv_ct+v2.getUv_ct)
                  v1.setDur_sum(v1.getDur_sum+v2.getDur_sum)
                  v1
              }
          },new ProcessWindowFunction[VisitorStats,VisitorStats,(String, String, String, String),TimeWindow] {

              override def process(key: (String, String, String, String), context: ProcessWindowFunction[VisitorStats, VisitorStats, (String, String, String, String), TimeWindow]#Context, elements: lang.Iterable[VisitorStats], out: Collector[VisitorStats]): Unit = {
                  val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
                  val iter = elements.iterator()
                  while (iter.hasNext){
                      val startDate = sdf.format(new Date(context.window.getStart))
                      val endDate = sdf.format(new Date(context.window.getEnd))
                      val elem = iter.next()
                      elem.setStt(startDate)
                      elem.setEdt(endDate)
                      out.collect(elem)
                  }
              }
          })
        env.execute()

    }
}
