package com.aifurion.app

import com.aifurion.beans.CleanedLogInfo
import com.aifurion.utils.{ETLUtil, JedisPoolUtil, MyKafkaUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis

object CountTotalClick {


    def main(args: Array[String]): Unit = {

        val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("test")
        val context = new StreamingContext(sparkConf, Seconds(2))

        //获得kafka输入流
        val inputDStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream("imooc", context)


        //数据清洗
        val cleanedLogInfo: DStream[CleanedLogInfo] = ETLUtil.getCleanedLog(inputDStream)


        val value: DStream[Long] = cleanedLogInfo.count()


        var jedis: Jedis = null
        try {
            value.foreachRDD(rdd => {

                rdd.foreach(partition => {
                    jedis = JedisPoolUtil.getConnection
                    jedis.hincrBy("value", "totalclick", partition)
                })

            })
        } catch {
            case ex: Exception =>
                ex.printStackTrace()
        } finally {
            if (jedis != null) jedis.close()
        }


        context.start()
        context.awaitTermination()

    }

}
