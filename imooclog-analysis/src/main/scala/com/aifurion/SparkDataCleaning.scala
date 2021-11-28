package com.aifurion

import com.aifurion.entity.BrowseFormatInfo
import com.aifurion.utils.ip.IpHelper
import com.aifurion.utils.{DateFormatUtil, PropertiesUtil, RegexUtil}
import org.apache.commons.lang3.StringUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

import java.util.Properties
import scala.util.matching.Regex


object SparkDataCleaning {

    private val properties: Properties = PropertiesUtil.getProperties


    private val struct: StructType = StructType(
        Array(
            StructField("url", StringType),
            StructField("cmsType", StringType),
            StructField("cmsId", LongType),
            StructField("flow", LongType),
            StructField("ip", StringType),
            StructField("city", StringType),
            StructField("time", StringType),
            StructField("day", StringType)
        )
    )


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

        val parsedRdd: RDD[Row] = formatRdd.map(line => parseLog(line))

        val parsedDF: DataFrame = spark.createDataFrame(parsedRdd, struct)


        //将清洗后的dataframe保存为parquet文件
        parsedDF.coalesce(1).
                write.
                partitionBy("day").
                parquet(properties.getProperty("file.protocol")+
                        properties.getProperty("file.dataCleaned.path"))

    }



    //对日志进行格式化，获得ip的城市，以及访问的具体行为
    def parseLog(log: List[String]): Row = {


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
                        case "video" | "code" | "learn" =>
                            val pattern: Regex = """(^[0-9]+)""".r
                            cmsId = pattern.findFirstIn(cmsTypeId(1)).get.toLong

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
                println("------------log----------------------")
                println(log)
                Row(0)
        }


    }


}
