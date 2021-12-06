package com.aifurion.app

import com.aifurion.beans.CleanedLogInfo
import com.aifurion.utils.{DateFormatUtil, ETLUtil, JedisPoolUtil, MyKafkaUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis

object TestRedisAnalysis {


    def main(args: Array[String]): Unit = {

        val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("test")
        val context = new StreamingContext(sparkConf, Seconds(5))

        //获得kafka输入流
        val inputDStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream("imooc", context)


        //数据清洗
        val cleanedLogInfo: DStream[CleanedLogInfo] = ETLUtil.getCleanedLog(inputDStream)


        val value: DStream[(String, Int)] = cleanedLogInfo.map(log => (log.city, 1)).reduceByKey(_ + _)


        value.foreachRDD(rdd =>
            rdd.foreachPartition { partitionOfRecords =>
                var jedis: Jedis = null

                try {
                    jedis = JedisPoolUtil.getConnection
                    partitionOfRecords.foreach(record => jedis.hincrBy("cityPopCount", record._1, record._2))
                } catch {
                    case ex: Exception =>
                        ex.printStackTrace()
                } finally {
                    if (jedis != null) jedis.close()
                }

            })

        context.start()
        context.awaitTermination()
    }

}
