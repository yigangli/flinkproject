package com.lyg.realtime.utils

import com.alibaba.fastjson.JSONObject
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.async.{ResultFuture, RichAsyncFunction}

import java.util
import java.util.concurrent.ExecutorService

/**
 * @author: yigang
 * @date: 2021/6/4
 * @desc: 自定义维度查询异步执行函数
 * RichAsyncFunction 里面的方法负责异步查询
 * DimJoinFunction 里面的方法负责将为表和主流进行关联
 */
abstract class DimAsyncFunction[T](var tableName:String) extends RichAsyncFunction[T,T] with DimJoinFunction[T] {
    var executorService:ExecutorService=_
    override def open(parameters: Configuration): Unit = {
        println("获得线程池!")
        executorService = ThreadPoolUtil.getInstance()
    }

    @throws(classOf[Exception])
    override def asyncInvoke(obj: T, resultFuture: ResultFuture[T])={
        executorService.submit(new Runnable {
            override def run(): Unit = {

                try {
                    val start = System.currentTimeMillis()
                    //从流对象中获取主键
                    val key = getKey(obj)
                    //根据主键获取维度对象数据
                    val dimJsonObject:JSONObject = DimUtil.getDimInfo(tableName,key)
                    println("dimJsonObject:"+dimJsonObject)
                    if(dimJsonObject!=null){
                        join(obj,dimJsonObject)
                    }
                    println("obj:"+obj)
                    val end = System.currentTimeMillis()
                    println("异步耗时:"+(end-start)+"毫秒")
                    resultFuture.complete(util.Arrays.asList(obj))

                }catch {
                    case e:Exception=>
                        println(String.format(tableName+"异步查询异常.%s",e))
                        e.printStackTrace()
                }
            }
        })
    }

}
