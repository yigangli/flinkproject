package com.lyg.realtime.common

/**
 * @author: yigang
 * @date: 2021/4/2
 * @desc:
 */
object Config {
  //hbase命名空间
  val hbase_Schema = "realtime"

  //phoenix连接字符串
  val phoenix_server = "jdbc:phoenix:cdh-node1:2181"
}
