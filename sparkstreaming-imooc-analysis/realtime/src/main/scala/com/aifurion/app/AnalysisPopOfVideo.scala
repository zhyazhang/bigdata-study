package com.aifurion.app

import com.aifurion.beans.CleanedLogInfo
import com.aifurion.utils.{ETLUtil, JedisPoolUtil, MyKafkaUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis

object AnalysisPopOfVideo {

    def main(args: Array[String]): Unit = {
        val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("test")
        val context = new StreamingContext(sparkConf, Seconds(5))

        //获得kafka输入流
        val inputDStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream("imooc", context)

        //数据清洗
        val cleanedLogInfo: DStream[CleanedLogInfo] = ETLUtil.getCleanedLog(inputDStream)

        val videoPopCount: DStream[(Long, Int)] = cleanedLogInfo.filter(log => "video".equals(log.cmsType))
                .map(log => (log.cmsId, 1))
                .reduceByKey(_ + _)

        videoPopCount.foreachRDD(rdd =>
            rdd.foreachPartition { partitionOfLogs => {
                var jedis: Jedis = null
                try {
                    jedis = JedisPoolUtil.getConnection
                    partitionOfLogs.foreach(record => jedis.hincrBy("videoPopCount", record._1.toString, record._2))
                } catch {
                    case ex: Exception =>
                        ex.printStackTrace()
                } finally {
                    if (jedis != null) jedis.close()
                }
            }
            }
        )

        context.start()
        context.awaitTermination()
    }
}
