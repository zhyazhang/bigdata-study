package com.aifurion.app

import com.aifurion.beans.CleanedLogInfo
import com.aifurion.utils.LogFormatToBean.parseLog
import com.aifurion.utils.ip.IpHelper
import com.aifurion.utils.{DateFormatUtil, MyKafkaUtil, RegexUtil}
import org.apache.commons.lang3.StringUtils
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.poi.ss.usermodel.Row
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object TestAnalysis {


    def main(args: Array[String]): Unit = {

        val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("test")
        val context = new StreamingContext(sparkConf, Seconds(5))

        //获得kafka输入流
        val inputDStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream("imooc", context)

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


        //格式化日志文件
        val cleanedLogInfo: DStream[CleanedLogInfo] = formated.map(line => parseLog(line))

        cleanedLogInfo.print()
        
        context.start()
        context.awaitTermination()
    }

}
