package com.aifurion

import com.aifurion.utils.ip.IpHelper
import com.aifurion.utils.{DateFormatUtil, PropertiesUtil, RegexUtil}
import org.apache.commons.lang3.StringUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SparkSession}

import java.util.Properties


object SparkDataCleaning {

    private val properties: Properties = PropertiesUtil.getProperties

    def main(args: Array[String]): Unit = {

        //获得sparkSession对象
        val spark: SparkSession = SparkSession.
                builder().
                appName("SparkDataCleaning").
                master("local[*]").
                getOrCreate()

        //读取原始日志文件
        val logDataRDD: RDD[String] = spark.
                sparkContext.
                textFile(properties.getProperty("file.protocol") + properties.getProperty("file.originalData.path"))


        /** 日志格式
         *
         * 0    ip     183.162.52.7
         * 1           -
         * 2           -
         * 3    time   [10/Nov/2016:00:01:02
         * 4    time   +0800]
         * 5           "POST
         * 6           /api3/getadv
         * 7           HTTP/1.1"
         * 8           200
         * 9    flow   813
         * 10          "www.imooc.com"
         * 11   url    "-"
         * 12          cid=0&timestamp=1478707261865&uid=2871142&marking=androidbanner&secrect=a6e8e14701ffe9f6063934780d9e2e6d&token=f51e97d1cb1a9caac669ea8acc162b96
         * 13          "mukewang/5.0.0
         * 14          (Android
         * 15          5.1.1;
         * 16          Xiaomi
         * 17          Redmi
         * 18          3
         * 19          Build/LMY47V),Network
         * 20          2G/3G"
         * 21          "-"
         * 22          10.100.134.244:80
         * 23          200
         * 24          0.027
         * 25          0.027
         *
         */


        val formatRdd: RDD[List[String]] = logDataRDD.map(line => {

            val fields: Array[String] = line.split(" ")
            val ip: String = fields(0)
            //转换日期格式

            val time: String = DateFormatUtil.parseDateTime(fields(3) + " " + fields(4))

            val flow: String = fields(9)
            val url: String = fields(11)

            List(time, url, flow, ip)
        }).
                //过滤掉内网IP
                filter(item => !"10.100.0.1".equals(item(3))).
                //过滤掉url为-等不规范的情况
                filter(item => !"-".equals(item(1)) && item(1).length > 10)

        formatRdd.map(line => parseLog(line)).take(10).foreach(println)

    }


    def parseLog(log: List[String]): Row = {


        try {

            val day: String = log.head.substring(0, 9)
            val time: String = log.head.substring(11, 18)


            val url: String = log(1)
            val flow: String = log(2)
            val ip: String = log(3)

            val city: String = IpHelper.findRegionByIp(ip)

            //http://www.imooc.com/code/547   ===>  code/547  547

            var cmsType = ""
            var cmsId = 0L

            val domain = "http://www.imooc.com/"
            val domainIndex: Int = url.indexOf(domain)
            if (domainIndex >= 0) {
                val cms: String = url.substring(domainIndex + domain.length).replace("\"", "")
                val cmsTypeId: Array[String] = cms.split("/")

                if (cmsTypeId.length > 1) {
                    cmsType = cmsTypeId(0)
                    cmsType match {
                        case "video" | "code" | "learn" => cmsId = cmsTypeId(1).split("\\?")(0).toLong
                        case "article" =>
                            val number: String = RegexUtil.findStartNumber(cmsTypeId(1))
                            if (StringUtils.isNotEmpty(number)) {
                                cmsId = number.toLong
                            }
                        case _ =>
                    }
                }
            }
            Row(url, cmsType, cmsId, flow, ip, city, time, day)
        } catch {
            case e: Exception =>
                //e.printStackTrace()
                //println("------------log----------------------")
                //println(log)
                Row(0)

        }


    }


}
