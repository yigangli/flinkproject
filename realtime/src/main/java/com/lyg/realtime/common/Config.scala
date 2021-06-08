package com.lyg.realtime.common

/**
 * @author: yigang
 * @date: 2021/4/2
 * @desc:
 */
object Config {

    //CheckPoint地址
    val checkpoint_url = "hdfs://cdh-node3:8020/home/flink/checkpoint/"
    //hbase命名空间
    val hbase_Schema = "realtime"

    //phoenix连接字符串
    val phoenix_server = "jdbc:phoenix:cdh-node1:2181"
}
