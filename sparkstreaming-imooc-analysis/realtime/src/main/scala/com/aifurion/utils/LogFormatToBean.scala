package com.aifurion.utils

import com.aifurion.beans.CleanedLogInfo
import com.aifurion.utils.ip.IpHelper
import org.apache.commons.lang3.StringUtils

object LogFormatToBean {


    //对日志进行格式化，获得ip的城市，以及访问的具体行为
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
                //e.printStackTrace()
                println("------------log----------------------")
                println(log)
                null
        }


    }

}
