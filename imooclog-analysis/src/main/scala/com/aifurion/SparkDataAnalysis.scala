package com.aifurion

import com.aifurion.entity.VideoPopularity
import com.aifurion.utils.PropertiesUtil
import org.apache.spark.sql.expressions.{Window, WindowSpec}
import org.apache.spark.sql.functions.{count, row_number, sum}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

import java.util.Properties
import scala.collection.mutable.ListBuffer

object SparkDataAnalysis {

    private val properties: Properties = PropertiesUtil.getProperties


    def main(args: Array[String]): Unit = {

        val spark: SparkSession = SparkSession.
                builder().
                appName("SparkDataAnalysis").
                master("local[*]").
                getOrCreate()

        //读取保存的数据
        val parsedDF: DataFrame = spark.
                read.
                parquet(properties.getProperty("file.protocol") +
                        properties.getProperty("file.dataCleaned.path"))

        //数据缓存

        parsedDF.show(10, truncate = false)


        val day: String = "2016-11-11"

        analysisDataByDay(spark, parsedDF, day, 10)

    }


    def analysisDataByDay(spark: SparkSession, df: DataFrame, day: String, topN: Int): Unit = {

        //一天观看数最多的视频-top n

        //analysisVideoPopularity(spark, df, day, topN)


        //analysisCityVideoPopularity(spark, df, day)

        //analysisVideoFLowPopularity(spark, df, day, topN)


        //analysisArticlePopularity(spark, df, day, topN)

        //analysisIp(spark, df, day, topN)


    }


    def analysisIp(spark: SparkSession, df: DataFrame, day: String, topN: Int): Unit = {

        import spark.implicits._

        val ipDF: Dataset[Row] = df.filter($"day" === day && $"cmsId" =!= 0)
                .groupBy("day", "ip")
                .agg(count("ip")
                        .as("times"))
                .orderBy($"times".desc)

        val window: WindowSpec = Window.partitionBy($"day").orderBy($"times".desc)

        val topIpDF: Dataset[Row] = ipDF
                .withColumn("top-n", row_number()
                        .over(window))
                .where($"top-n" <= topN)

        topIpDF.show()
    }


    def analysisArticlePopularity(spark: SparkSession, df: DataFrame, day: String, topN: Int): Unit = {

        import spark.implicits._

        val articlePopDF: Dataset[Row] = df.filter($"day" === day && $"cmsType" === "article" && $"cmsId" =!= 0)
                .groupBy("day", "cmsId")
                .agg(count("cmsId")
                        .as("times"))
                .orderBy($"times".desc)

        articlePopDF.show(10)


    }


    def analysisVideoFLowPopularity(spark: SparkSession, df: DataFrame, day: String, topN: Int): Unit = {
        import spark.implicits._

        val flowVideoPopDF: Dataset[Row] = df
                .filter($"day" === day && $"cmsType" === "video" && $"cmsId" =!= 0)
                .groupBy("day", "cmsId")
                .agg(sum("flow")
                        .as("flows"))
                .orderBy($"flows".desc)


        /** flowVideoPopDF.show()
         * +----------+-----+-------+
         * |       day|cmsId|  flows|
         * +----------+-----+-------+
         * |2016-11-11| 6062|3145728|
         * |2016-11-11|12418|2641096|
         * |2016-11-11| 5729|1572864|
         * |2016-11-11| 4963|1048576|
         * |2016-11-11|13171|1048576|
         * |2016-11-11| 7854| 805714|
         * |2016-11-11| 3510| 792130|
         * |2016-11-11| 4586| 554806|
         * |2016-11-11| 2216| 524342|
         * |2016-11-11| 4350| 524342|
         * |2016-11-11|10252| 524288|
         * |2016-11-11|12865| 524288|
         * |2016-11-11| 1412|  83954|
         * |2016-11-11| 9016|  82137|
         * |2016-11-11| 1441|  71629|
         * |2016-11-11| 1414|  54793|
         * |2016-11-11| 6165|  34655|
         * |2016-11-11| 8697|  31357|
         * |2016-11-11| 9009|  30572|
         * |2016-11-11|10216|  30139|
         * +----------+-----+-------+
         */


    }


    def analysisCityVideoPopularity(spark: SparkSession, df: DataFrame, day: String): Unit = {

        import spark.implicits._

        val cityVideoPopDF: Dataset[Row] = df
                .filter($"day" === day && $"cmsType" === "video" && $"cmsId" =!= 0)
                .groupBy("city", "day", "cmsId")
                .agg(count("cmsId").
                        as("times")).
                orderBy($"times".desc)

        //cityVideoPopDF.show()

        cityVideoPopDF.select(
            cityVideoPopDF("day"),
            cityVideoPopDF("city"),
            cityVideoPopDF("cmsId"),
            cityVideoPopDF("times"),
            row_number()
                    .over(Window.partitionBy(cityVideoPopDF("city"))
                            .orderBy(cityVideoPopDF("times").desc))
                    .as("times_rank")
        ).filter("times_rank <= 3")

        /** show(9, truncate = false)
         * +----------+------+-----+-----+----------+
         * |day       |city  |cmsId|times|times_rank|
         * +----------+------+-----+-----+----------+
         * |2016-11-11|北京市|7854 |48   |1         |
         * |2016-11-11|北京市|6062 |6    |2         |
         * |2016-11-11|北京市|12418|5    |3         |
         * |2016-11-11|辽宁省|6701 |2    |1         |
         * |2016-11-11|辽宁省|2469 |1    |2         |
         * |2016-11-11|辽宁省|42   |1    |3         |
         * |2016-11-11|浙江省|3510 |49   |1         |
         * |2016-11-11|浙江省|7289 |6    |2         |
         * |2016-11-11|浙江省|7292 |3    |3         |
         * +----------+------+-----+-----+----------+
         *
         */


    }

    def analysisVideoPopularity(spark: SparkSession, df: DataFrame, day: String, topN: Int): Unit = {

        import spark.implicits._

        val videoPopDF: Dataset[Row] = df.filter($"day" === day && $"cmsType" === "video" && $"cmsId" =!= 0)
                .groupBy("day", "cmsId")
                .agg(count("cmsId")
                        .as("times"))
                .orderBy($"times".desc)

        /** videoPopDF.show(topN)
         * +----------+-----+-----+
         * |       day|cmsId|times|
         * +----------+-----+-----+
         * |2016-11-11| 3510|   50|
         * |2016-11-11| 7854|   48|
         * |2016-11-11| 1412|   34|
         * |2016-11-11| 1441|   28|
         * |2016-11-11| 8865|   21|
         * |2016-11-11| 1414|   18|
         * |2016-11-11| 9009|   18|
         * |2016-11-11|10216|   17|
         * |2016-11-11| 4586|   17|
         * |2016-11-11|12418|   17|
         * +----------+-----+-----+
         */

        val window: WindowSpec = Window.partitionBy($"day").orderBy($"times".desc)
        val videoPopTopN: Dataset[Row] = videoPopDF.
                withColumn("top-n", row_number().over(window)).
                where($"top-n" <= topN)

        videoPopTopN.show()

        //统计结果写入到mysql

        /*try {
            videoPopTopN.rdd.foreachPartition(partition => {
                val list = new ListBuffer[VideoPopularity]
                partition.foreach(row =>{
                    val day: String = row.getAs[String]("day")
                    val cmsId: Long = row.getAs[Long]("cmsId")
                    val times: Long = row.getAs[Long]("times")
                    list.append(VideoPopularity(day,cmsId,times))
                })

            })

        } catch {
            case e: Exception => e.printStackTrace()
        }*/


    }

}
