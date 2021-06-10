package com.lyg.realtime.utils

import java.time.{LocalDateTime, ZoneId, ZoneOffset}
import java.time.format.DateTimeFormatter
import java.util.Date

/**
 * @author: yigang
 * @date: 2021/6/8
 * @desc: JDK8 的 DateTimeFormatter 替换 SimpleDateFormat，因为 SimpleDateFormat 存在线程安全问题
 */
object DateTimeUtil {

    val formator = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")

    def toYMDhms(date:Date)={
        val localDateTime = LocalDateTime.ofInstant(date.toInstant(),ZoneId.systemDefault())
        formator.format(localDateTime)
    }
    def toTs(YmDHms:String):Long = {
        val localDateTime = LocalDateTime.parse(YmDHms,formator)
        localDateTime.toInstant(ZoneOffset.of("+8")).toEpochMilli
    }
}
