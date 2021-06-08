package com.lyg.realtime.utils

import com.alibaba.fastjson.JSONObject

/**
 * @author: yigang
 * @date: 2021/6/4
 * @desc: 维度查询关联的接口
 */
trait DimJoinFunction[T] {

    /**
     * 需要实现如何把结果装配给数据流对象
     * @param t 数据流对象
     * @param jsonObject 异步查询结果
     * @throws java.lang.Exception
     */
    @throws(classOf[Exception])
    def join(t:T,jsonObject:JSONObject)

    /**
     * 需要实现如何从流中对象获取主键
     * @param t 数据流对象
     */
    def getKey(t:T):String
}
