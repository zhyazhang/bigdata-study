package com.aifurion.app

import com.aifurion.beans.CleanedLogInfo
import com.aifurion.utils.{DateFormatUtil, ETLUtil, JedisPoolUtil, MyKafkaUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis

object AnalysisPVOfProvince {


    def main(args: Array[String]): Unit = {

        val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("test")
        val context = new StreamingContext(sparkConf, Seconds(1))

        //获得kafka输入流
        val inputDStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream("imooc", context)


        //数据清洗
        val cleanedLogInfo: DStream[CleanedLogInfo] = ETLUtil.getCleanedLog(inputDStream)

        //统计每个省的浏览量
        //将省份转为Tuple,河南=>(河南,1)
        //并进行归并
        val value: DStream[(String, Int)] = cleanedLogInfo.map(log => (log.city, 1)).reduceByKey(_ + _)


        //将信息写入redis
        //hincrBy
        //Redis Hincrby 命令用于为哈希表中的字段值加上指定增量值。
        //增量也可以为负数，相当于对指定字段进行减法操作。
        //如果哈希表的 key 不存在，一个新的哈希表被创建并执行 HINCRBY 命令。

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
