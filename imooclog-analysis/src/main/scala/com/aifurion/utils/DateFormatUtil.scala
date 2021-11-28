package com.aifurion.utils

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import java.util.Locale

/**
 * @author zzy
 * 日期格式化工具类
 *
 */
object DateFormatUtil {

    //使用java8线程安全类DateTimeFormatter做时间格式化
    private val formatter: DateTimeFormatter = DateTimeFormatter.
            ofPattern("dd/MMM/yyyy:HH:mm:ss Z", Locale.ENGLISH)

    def parseDateTime(time: String): String = {

        try {
            val ftime: String = LocalDateTime.
                    parse(time.substring(time.indexOf("[") + 1, time.lastIndexOf("]")), formatter).
                    toString
            if (ftime.length == 16) ftime + ":00"
            else ftime

        } catch {
            case e: Exception => ""
        }
    }


    def main(args: Array[String]): Unit = {


        println(parseDateTime("[10/Nov/2016:07:28:00 +0800]"))
    }
}
