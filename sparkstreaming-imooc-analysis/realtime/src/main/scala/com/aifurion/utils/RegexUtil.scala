package com.aifurion.utils

import scala.util.matching.Regex


object RegexUtil {

    val pattern: Regex = "^\\d+".r
    /**
     * 获取字符串开头的数字，找不到数字则返回 None
     */
    def findStartNumber(str: String): String = {
        val number: Option[String] = pattern.findFirstIn(str)
        if (number.isEmpty) "" else number.get
    }


    def main(args: Array[String]): Unit = {

    }

}
