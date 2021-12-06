package com.aifurion.utils

import com.aifurion.beans.CleanedLogInfo
import com.aifurion.utils.ip.IpHelper
import org.apache.commons.lang3.StringUtils
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.streaming.dstream.{DStream, InputDStream}


//实时数据清洗工具类
object ETLUtil {



    //获得清洗后的日志信息
    def getCleanedLog(input: InputDStream[ConsumerRecord[String, String]]): DStream[CleanedLogInfo] ={

        val formatedLog: DStream[List[String]] = getFormatedLog(input)

        formatedLog.map(line => parseLog(line))

    }

    //清洗第一步
    // 简单格式化，并过滤不需要的数据

    //结果： 2016-11-10 00:10:09	http://www.imooc.com/video/7533	54	106.120.213.44

    def getFormatedLog(inputDStream: InputDStream[ConsumerRecord[String, String]]): DStream[List[String]] = {

        val formated: DStream[List[String]] = inputDStream.map {

            line => {
                val logString: String = line.value()
                val fields: Array[String] = logString.split(" ")
                val ip: String = fields(0)
                //转换日期格式
                val time: String = DateFormatUtil.parseDateTime(fields(3) + " " + fields(4))
                val flow: String = fields(9)
                val url: String = fields(11)

                List(time, url, flow, ip)
            }
        }
                //过滤掉内网IP
                .filter(item => !"10.100.0.1".equals(item(3)))
                //过滤掉url为-等不规范的情况
                .filter(item => !"-".equals(item(1)) && item(1).length > 10)
        formated

    }


    //清洗第二步
    //对日志进行格式化，获得ip的城市，以及访问的具体行为

    //结果：CleanedLogInfo("http://www.imooc.com/learn/9",learn,9,4468,120.27.173.206,上海市,00:03:14,2016-11-10)
    def parseLog(log: List[String]): CleanedLogInfo = {

        try {
            val day: String = log.head.substring(0, 10)
            val time: String = log.head.substring(11, 19)

            val url: String = log(1)
            val flow: Long = log(2).toLong
            val ip: String = log(3)

            val city: String = IpHelper.findRegionByIp(ip)

            //http://www.imooc.com/code/547   ===>  code/547  547

            var cmsType: String = ""
            var cmsId: Long = 0L

            val domain = "http://www.imooc.com/"
            val domainIndex: Int = url.indexOf(domain)
            if (domainIndex >= 0) {
                val cms: String = url.substring(domainIndex + domain.length).replace("\"", "")
                val cmsTypeId: Array[String] = cms.split("/")

                if (cmsTypeId.length > 1) {
                    cmsType = cmsTypeId(0)
                    cmsType match {
                        case "video" | "code" | "learn" | "article" =>
                            val number: String = RegexUtil.findStartNumber(cmsTypeId(1))
                            if (StringUtils.isNotEmpty(number)) {
                                cmsId = number.toLong
                            }
                        case _ =>
                    }
                }
            }
            CleanedLogInfo(url, cmsType, cmsId, flow, ip, city, time, day)
        } catch {
            case e: Exception =>
                e.printStackTrace()
                println("------------log----------------------")
                println(log)
                null
        }
    }
}
