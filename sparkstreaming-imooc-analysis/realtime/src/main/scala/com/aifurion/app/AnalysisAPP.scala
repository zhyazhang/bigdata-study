package com.aifurion.app

import com.aifurion.beans.CleanedLogInfo
import com.aifurion.utils.{ETLUtil, JedisPoolUtil, MyKafkaUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import redis.clients.jedis.Jedis

object AnalysisAPP {

    def main(args: Array[String]): Unit = {

        val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("test")
        val context = new StreamingContext(sparkConf, Seconds(2))

        //获得kafka输入流
        val inputDStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream("imooc", context)

        //数据清洗
        val cleanedLogInfo: DStream[CleanedLogInfo] = ETLUtil.getCleanedLog(inputDStream)


        /**
         * 统计视频受欢迎程度
         */
        val videoPopCount: DStream[(Long, Int)] = cleanedLogInfo.filter(log => "video".equals(log.cmsType))
                .map(log => (log.cmsId, 1))
                .reduceByKey(_ + _)

        //统计视频受欢迎程度，即统计video浏览量
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

        /**
         * 统计总浏览量和实时浏览量
         *
         */
        val countValue: DStream[Long] = cleanedLogInfo.count()
        var jedis: Jedis = null
        try {
            //求一个等待窗时间内的总点击量
            countValue.foreachRDD(rdd => {
                rdd.foreach(partition => {
                    jedis = JedisPoolUtil.getConnection
                    //累加点击量
                    jedis.hincrBy("value", "totalclick", partition)
                    //记录每个事件窗的瞬时点击量
                    jedis.hset("value", "perclick", partition + "")
                })
            })

        } catch {
            case ex: Exception =>
                ex.printStackTrace()
        } finally {
            if (jedis != null) jedis.close()
        }

        /**
         * 统计每个省的浏览量
         *
         */

        val mapValue: DStream[(String, Int)] = cleanedLogInfo.map(log => (log.city, 1)).reduceByKey(_ + _)

        //将信息写入redis
        //hincrBy
        //Redis Hincrby 命令用于为哈希表中的字段值加上指定增量值。
        //增量也可以为负数，相当于对指定字段进行减法操作。
        //如果哈希表的 key 不存在，一个新的哈希表被创建并执行 HINCRBY 命令。

        mapValue.foreachRDD(rdd =>
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
