package com.lyg.realtime.app.func

import com.alibaba.fastjson.JSONObject
import com.lyg.realtime.common.Config
import com.lyg.realtime.utils.DimUtil
import org.apache.commons.lang3.StringUtils
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}

import java.sql.{Connection, DriverManager, PreparedStatement}

/**
 * @author: yigang
 * @date: 2021/4/6
 * @desc:
 */
class DimSink extends RichSinkFunction[JSONObject] {
    var conn: Connection = null

    override def open(parameters: Configuration): Unit = {
        Class.forName("org.apache.phoenix.jdbc.PhoenixDriver")
        conn = DriverManager.getConnection(Config.phoenix_server)
    }

    override def invoke(jsonObject: JSONObject, context: SinkFunction.Context): Unit = {
        //获取目标表的名称
        val tableName = jsonObject.getString("sink_table")
        //获取json中data数据
        val dataJsonObj = jsonObject.getJSONObject("data")

        if (dataJsonObj != null && dataJsonObj.size() >= 0) {
            val sql = getUpsertSql(tableName.toUpperCase, dataJsonObj)

            var ps: PreparedStatement = null
            try {
                ps = conn.prepareStatement(sql)

                ps.executeUpdate()
                //执行完phoenix插入操作之后，需要手动提交事务
                conn.commit()
            } catch {
                case exception: Exception => exception.printStackTrace()
                    throw new RuntimeException("向phoenix插入数据失败")
            } finally {
                if (ps != null) {
                    ps.close()
                }
            }
        }
        //如果维度发生变化，清空在redis中缓存该key的值
        if(jsonObject.getString("type").equals("update")||jsonObject.getString("type").equals("delete")){
            DimUtil.deleteCached(tableName,dataJsonObj.getString("id"))
        }

    }

    def getUpsertSql(tableName: String, jsonObj: JSONObject) = {
        val keys = jsonObj.keySet()
        val values = jsonObj.values()

        "upsert into " + Config.hbase_Schema + "." + tableName + "(" +
          StringUtils.join(keys, ",") + ")" +
          "values ('" + StringUtils.join(values, "','") + "')"
    }
}
