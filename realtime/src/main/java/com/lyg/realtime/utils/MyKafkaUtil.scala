package com.lyg.realtime.utils

import org.apache.flink.api.common.serialization.{SerializationSchema, SimpleStringSchema}
import org.apache.flink.streaming.connectors.kafka
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer, FlinkKafkaProducer, KafkaSerializationSchema}
import org.apache.flink.streaming.util.serialization.KeyedSerializationSchema
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.ProducerConfig

import java.util.Properties

/**
 * @author: yigang
 * @date: 2021/3/29
 * @desc: 操作kafka的工具类
 */
object MyKafkaUtil {
    val kafkaServer = "cdh-node6:9092,cdh-node8:9092,cdh-node9:9092"
    val DEFAULT_TOPIC = "DEFAULT_DATA"
    def getKafkaSource(topic: String, groupid: String) = {
        val prop = new Properties()
        prop.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaServer)
        prop.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupid)
        new FlinkKafkaConsumer[String](topic, new SimpleStringSchema(), prop)
    }
    def getKafkaSink(topic:String)={
      new FlinkKafkaProducer[String](kafkaServer,topic,new SimpleStringSchema())
    }

    def getKafkaSinkBySchema[T](kafkaSerializationSchema:KafkaSerializationSchema[T])={
      val prop = new Properties()
      prop.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaServer)
      prop.setProperty(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG, 15*60*1000+"")
      new FlinkKafkaProducer[T](DEFAULT_TOPIC,kafkaSerializationSchema,prop,FlinkKafkaProducer.Semantic.EXACTLY_ONCE)
    }
}