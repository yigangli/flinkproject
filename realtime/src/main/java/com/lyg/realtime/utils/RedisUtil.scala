package com.lyg.realtime.utils

import redis.clients.jedis.{Jedis, JedisPool, JedisPoolConfig}

/**
 * @author: yigang
 * @date: 2021/5/18
 * @desc: 通过连接池获取Jedis的工具类
 */
object RedisUtil {

  var jedisPool:JedisPool = null
  def getJedis():Jedis={
    if(jedisPool==null) {
      val jedisPoolConfig: JedisPoolConfig = new JedisPoolConfig
      jedisPoolConfig.setMaxTotal(100) //最大链接数
      jedisPoolConfig.setBlockWhenExhausted(true) //连接数用完是否等待
      jedisPoolConfig.setMaxWaitMillis(2000) //等待时间

      jedisPoolConfig.setMaxIdle(5) //最大闲置连接数
      jedisPoolConfig.setMinIdle(5) //最小闲置连接数
      jedisPoolConfig.setTestOnBorrow(true) //取连接的时候进行一下测试

      jedisPool = new JedisPool(jedisPoolConfig, "hostname", 1234, 1000) //主机，端口，连接超时时间
      println("开辟连接池")
      jedisPool.getResource
    }else{
      jedisPool.getResource
    }
  }
}
