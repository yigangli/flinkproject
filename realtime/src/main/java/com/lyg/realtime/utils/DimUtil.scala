package com.lyg.realtime.utils

import com.alibaba.fastjson.{JSON, JSONObject}
import redis.clients.jedis.Jedis

/**
 * @author: yigang
 * @date: 2021/5/17
 * @desc:
 */
object DimUtil {

    def main(args: Array[String]): Unit = {
        val array: Array[(String, String)] = Array(("id", "12"))
        //    val jsonObj = getDimInfoNoCache("dim_base_trademark",array)
        //    println(jsonObj.toJSONString)
        val json = getDimInfo("dim_base_trademark", array)
        println(json)
    }

    def getDimInfoNoCache(tableName: String, colNameAndValue: Array[(String, String)]): JSONObject = {
        var whereSql = " where "
        for (i <- colNameAndValue.indices) {
            val colName = colNameAndValue(i)._1
            val colValue = colNameAndValue(i)._2
            if (i != 0) {
                whereSql += " and "
            }
            whereSql = whereSql + colName + " = '" + colValue + "'"
        }
        val sql = "select * from " + tableName + whereSql
        var dimInfoJsonObj: JSONObject = null
        val list: List[JSONObject] = PhoenixUtil.queryList(sql, classOf[JSONObject])
        if (list != null && list.nonEmpty) {
            dimInfoJsonObj = list.head
        } else {
            println("维度数据未找到:" + sql)
        }
        dimInfoJsonObj
    }

    def getDimInfo(tableName:String,id:String): JSONObject ={
        val arr = Array[(String,String)]{("id",id)}
        getDimInfo(tableName,arr)
    }
    def getDimInfo(tableName: String, colNameAndValue: Array[(String, String)]): JSONObject = {
        var whereSql = " where "
        var redisKey = ""
        for (i <- colNameAndValue.indices) {
            val name = colNameAndValue(i)._1
            val value = colNameAndValue(i)._2
            if (i > 0) {
                whereSql += "and "
                redisKey += "_"
            }

            whereSql = whereSql + name + "='" + value + "'"
            redisKey += value
        }
        var jedis: Jedis = null
        var dimJson: String = null
        var dimInfo: JSONObject = null
        val key = "dim:" + tableName.toLowerCase() + ":" + redisKey
        try {
            // 从连接池获取连接
            val jedis = RedisUtil.getJedis()
            dimJson = jedis.get(key)
        } catch {
            case e: Exception => e.printStackTrace()
        }
        if (dimJson != null) {
            println("缓存中存在，直接从redis获取结果")
            dimInfo = JSON.parseObject(dimJson)
        } else {
            println("缓存中未查到，从phoeinx中查")
            dimInfo = getDimInfoNoCache(tableName, colNameAndValue)
            if (jedis != null) {
                jedis.setex(key, 3600 * 24, dimInfo.toJSONString)
                println("将phoeinx中查到的值放入缓存，保留1天")
            }else{
                println("不存在缓存池,无法将本次查询结果放入redis")
            }
        }
        if(jedis!=null){
            jedis.close()
            println("关闭缓存连接")
        }
        dimInfo
    }
    def deleteCached(tableName:String,id:String)={
        val key = "dim:" + tableName.toLowerCase() + ":" + id
        try{
            val jedis = RedisUtil.getJedis()
            jedis.del(key)
            jedis.close()
        }catch {
           case e:Exception=>{
               println("缓存异常！")
               e.printStackTrace()
           }
        }
    }
}
