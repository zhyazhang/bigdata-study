

<img src="../images/spark-logo.png" alt="Apache Spark"/>



## 1 项目简介

Spark SQL 分析 Imooc 访问日志



:seedling: 环境说明：

三台Vmware centos7组成的hadoop集群，spark运行模式为yarn client，代码执行在zeppelin端

- hadoop 3.2.0

- zookeeper 3.6.3

- spark 3.0.0

- zeppelin 0.10.0



## 2 数据处理

### 2.1 数据介绍

**Imooc 访问日志文件**：access.20161111.log

**数据量**：一千万条访问日志，未作数据清洗

**下载地址**： https://pan.baidu.com/s/1WBhvSV4oEKKGg0Rk_QcN9A  提取码： 9rv5

**日志格式：**

```
183.162.52.7 - - [10/Nov/2016:00:01:02 +0800] "POST /api3/getadv HTTP/1.1" 200 813 "www.imooc.com" "-" cid=0&timestamp=1478707261865&uid=2871142&marking=androidbanner&secrect=a6e8e14701ffe9f6063934780d9e2e6d&token=f51e97d1cb1a9caac669ea8acc162b96 "mukewang/5.0.0 (Android 5.1.1; Xiaomi Redmi 3 Build/LMY47V),Network 2G/3G" "-" 10.100.134.244:80 200 0.027 0.027
```

### 2.2 数据清洗

关键程序：

```scala
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
                parquet(properties.getProperty("file.protocol") +
                        properties.getProperty("file.dataCleaned.path"))


//对日志进行格式化，获得ip的城市，以及访问的具体行为
    def parseLog(log: List[String]): Row = {


        try {

            val day: String = log.head.substring(0, 10)
            val time: String = log.head.substring(11, 19)
            val url: String = log(1)
            val flow: Long = log(2).toLong
            val ip: String = log(3)
            val city: String = IpHelper.findRegionByIp(ip)

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
            Row(url, cmsType, cmsId, flow, ip, city, time, day)
        } catch {
            case e: Exception =>
                //e.printStackTrace()
                println("------------log----------------------")
                println(log)
                Row(0)
        }
    }
```

清洗后的数据：

```
+-----------------------------------+-------+-----+----+---------------+------+--------+----------+
|url                                |cmsType|cmsId|flow|ip             |city  |time    |day       |
+-----------------------------------+-------+-----+----+---------------+------+--------+----------+
|"http://www.imooc.com/code/1852"   |code   |1852 |2345|117.35.88.11   |陕西省|00:01:02|2016-11-10|
|"http://www.imooc.com/code/2053"   |code   |2053 |331 |211.162.33.31  |福建省|00:01:02|2016-11-10|
|"http://www.imooc.com/code/3500"   |code   |3500 |54  |116.22.196.70  |广东省|00:01:02|2016-11-10|
|"http://www.imooc.com/code/547"    |code   |547  |54  |119.130.229.90 |广东省|00:01:02|2016-11-10|
|"http://www.imooc.com/code/2053"   |code   |2053 |2954|211.162.33.31  |福建省|00:01:02|2016-11-10|
|"http://www.imooc.com/video/8701"  |video  |8701 |54  |106.39.41.166  |北京市|00:01:02|2016-11-10|
|"http://www.imooc.com/ceping/4191" |ceping |0    |7227|39.186.247.142 |浙江省|00:01:02|2016-11-10|
|"http://www.imooc.com/video/5915/0"|video  |5915 |54  |113.140.11.123 |陕西省|00:01:02|2016-11-10|
|"http://www.imooc.com/code/75"     |code   |75   |2152|125.119.9.35   |浙江省|00:01:02|2016-11-10|
|"http://www.imooc.com/video/9819"  |video  |9819 |54  |125.122.216.102|浙江省|00:01:02|2016-11-10|
+-----------------------------------+-------+-----+----+---------------+------+--------+----------+
```

## 3 数据分析

### 3.1 一天中最受欢迎的视频

```scala
def analysisVideoPopularity(spark: SparkSession, df: DataFrame, day: String, topN: Int): Unit = {
        import spark.implicits._
    
        val videoPopDF: Dataset[Row] = df.filter($"day" === day && $"cmsType" === "video" && $"cmsId" =!= 0)
                .groupBy("day", "cmsId")
                .agg(count("cmsId")
                        .as("times"))
                .orderBy($"times".desc)
        val window: WindowSpec = Window.partitionBy($"day").orderBy($"times".desc)
        val videoPopTopN: Dataset[Row] = videoPopDF.
                withColumn("top-n", row_number().over(window)).
                where($"top-n" <= topN)
    }
```

```
+----------+-----+-----+-----+
|       day|cmsId|times|top-n|
+----------+-----+-----+-----+
|2016-11-11| 3510|   50|    1|
|2016-11-11| 7854|   48|    2|
|2016-11-11| 1412|   34|    3|
|2016-11-11| 1441|   28|    4|
|2016-11-11| 8865|   21|    5|
|2016-11-11| 9009|   18|    6|
|2016-11-11| 1414|   18|    7|
|2016-11-11| 4586|   17|    8|
|2016-11-11|12418|   17|    9|
|2016-11-11|10216|   17|   10|
+----------+-----+-----+-----+
```



### 3.2 某天某城市最受欢迎的视频

```scala
def analysisCityVideoPopularity(spark: SparkSession, df: DataFrame, day: String): Unit = {

        import spark.implicits._

        val cityVideoPopDF: Dataset[Row] = df
                .filter($"day" === day && $"cmsType" === "video" && $"cmsId" =!= 0)
                .groupBy("city", "day", "cmsId")
                .agg(count("cmsId").
                        as("times")).
                orderBy($"times".desc)
    
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
         .show(9, truncate = false)
    }
```

```
+----------+------+-----+-----+----------+
|day       |city  |cmsId|times|times_rank|
+----------+------+-----+-----+----------+
|2016-11-11|北京市|7854 |48   |1         |
|2016-11-11|北京市|6062 |6    |2         |
|2016-11-11|北京市|12418|5    |3         |
|2016-11-11|辽宁省|6701 |2    |1         |
|2016-11-11|辽宁省|2469 |1    |2         |
|2016-11-11|辽宁省|42   |1    |3         |
|2016-11-11|浙江省|3510 |49   |1         |
|2016-11-11|浙江省|7289 |6    |2         |
|2016-11-11|浙江省|7292 |3    |3         |
+----------+------+-----+-----+----------+
```



### 3.3 按流量计算视频的受欢迎程度

```scala
def analysisVideoFLowPopularity(spark: SparkSession, df: DataFrame, day: String, topN: Int): Unit = {
        import spark.implicits._

        val flowVideoPopDF: Dataset[Row] = df
                .filter($"day" === day && $"cmsType" === "video" && $"cmsId" =!= 0)
                .groupBy("day", "cmsId")
                .agg(sum("flow")
                        .as("flows"))
                .orderBy($"flows".desc)
                .show()
    }
```

```
+----------+-----+-------+
|       day|cmsId|  flows|
+----------+-----+-------+
|2016-11-11| 6062|3145728|
|2016-11-11|12418|2641096|
|2016-11-11| 5729|1572864|
|2016-11-11| 4963|1048576|
|2016-11-11|13171|1048576|
|2016-11-11| 7854| 805714|
|2016-11-11| 3510| 792130|
|2016-11-11| 4586| 554806|
|2016-11-11| 2216| 524342|
|2016-11-11| 4350| 524342|
|2016-11-11|10252| 524288|
|2016-11-11|12865| 524288|
|2016-11-11| 1412|  83954|
|2016-11-11| 9016|  82137|
|2016-11-11| 1441|  71629|
|2016-11-11| 1414|  54793|
|2016-11-11| 6165|  34655|
|2016-11-11| 8697|  31357|
|2016-11-11| 9009|  30572|
|2016-11-11|10216|  30139|
+----------+-----+-------+
```



### 3.4 统计看视频最多的IP

```scala
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
```

```
+----------+---------------+-----+-----+
|       day|             ip|times|top-n|
+----------+---------------+-----+-----+
|2016-11-11|   112.35.19.86|   56|    1|
|2016-11-11| 42.228.225.198|   52|    2|
|2016-11-11|     49.4.158.0|   50|    3|
|2016-11-11| 122.224.33.168|   50|    4|
|2016-11-11|120.198.231.150|   44|    5|
|2016-11-11|116.227.234.111|   35|    6|
|2016-11-11| 111.26.181.152|   29|    7|
|2016-11-11| 119.98.115.244|   26|    8|
|2016-11-11|  116.227.193.2|   26|    9|
|2016-11-11|  218.249.50.51|   24|   10|
+----------+---------------+-----+-----+
```

