package com.lyg.realtime.utils

import java.util.concurrent.{LinkedBlockingDeque, ThreadPoolExecutor, TimeUnit}

/**
 * @author: yigang
 * @date: 2021/6/3
 * @desc:
 */
object ThreadPoolUtil {
    var pool:ThreadPoolExecutor=_
    def getInstance(): ThreadPoolExecutor ={
        if(pool==null){
            ThreadPoolUtil.getClass.synchronized{
                if (pool==null){
                    println("开辟线程池")
                    /*
                    ThreadPoolExecutor(
                    corePoolSize: Int,  指定了线程池中的线程数量，它的数量决定了添加的任务是开辟新线程去执行还是放到workQueue任务队列中去
                    maximumPoolSize: Int,  指定了线程池中的最大线程数量，这个参数会根据你使用的workQueue任务队列的类型，决定线程池会开辟的最大线程数量
                    keepAliveTime: Long, 当线程池中空闲线程数量超过corePoolSize时，多余的线程会在多长时间内被销毁
                    unit: TimeUnit, keepAliveTime的时间单位
                    workQueue: BlockingQueue[Runnable]任务队列，被添加到线程池中，但尚未被执行的任务)
                     */
                    pool = new ThreadPoolExecutor(4,20,300,TimeUnit.SECONDS,
                        new LinkedBlockingDeque[Runnable](Integer.MAX_VALUE))

                }
            }
        }
        pool
    }

    def main(args: Array[String]): Unit = {
        getInstance()
        print("开辟成功")
    }
}
