package com.lyg.realtime.utils

import java.time.{LocalDateTime, ZoneId, ZoneOffset}
import java.time.format.DateTimeFormatter
import java.util.Date

/**
 * @author: yigang
 * @date: 2021/6/8
 * @desc:
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
