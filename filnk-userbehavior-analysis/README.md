<img src="../images/flink-logo.png" alt="Apache Flink"/>

:seedling: 持续更新中。。。



## 1 数据处理

采用和[userbehavior-analysis](https://github.com/metaword/bigdata-study/tree/master/userbehavior-analysis)相同的数据，但是由于数据乱序太大，几乎无法正常使用。

所以采用在将数据写入kafka时替换时间戳为实时时间。



### 1.1 配置kafka集群

**启动zookeeper集群、kafka集群**

**创建behavior topic**

```shell
/opt/module/kafka/bin/kafka-topics.sh --create --zookeeper hadoop102:2181 --replication-factor 3 --partitions 1 --topic behavior
```

### 1.2 启动数据生成器

```java
public class SourceGenerator {

    public static void main(String[] args) throws Exception {
        
        if (args.length != 1) {
            throw new Exception("parameter number does not match\n arg1: file path");
        }
        try (InputStream inputStream = new FileInputStream(args[0])) {
            BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));

            while (reader.ready()) {
                StringBuilder line = new StringBuilder(reader.readLine());
                long timeStamp = System.currentTimeMillis();
                int length = line.length();
                line.replace(length - 10, length, String.valueOf(timeStamp));
                System.out.println(line);
                Thread.sleep(1);
            }
            reader.close();
        } catch (IOException e) {
            throw new RuntimeException();
        }
    }
}
```

>将代码打成jar包发送到服务器，执行命令：

```shell
java -cp mock-source-generator-1.0.jar com.aifurion.SourceGenerator /pathToFile | /opt/module/kafka/bin/kafka-console-producer.sh --broker-list hadoop102:9092 --topic behavior
```

>其中**pathToFile**为要生成的userbehavior文件路径
>
>如：/home/metaword/mock/UserBehavior.csv



## 2 热点商品

**关键代码**

### 2.1 转换为pojo，分配时间戳和watermaker

```java
SingleOutputStreamOperator<UserBehavior> dataStream =
        inputStream.map(
                line -> {
                    String[] fields = line.split(",");
                    return new UserBehavior(new Long(fields[0]), new Long(fields[1]),
                            new Long(fields[2]), fields[3], new Long(fields[4]));
                }
        ).assignTimestampsAndWatermarks(new AscendingTimestampExtractor<UserBehavior>() {
            @Override
            public long extractAscendingTimestamp(UserBehavior element) {
                return element.getTimestamp();
            }
        });
```

### 2.2分组开窗聚合

```java
SingleOutputStreamOperator<ItemViewCount> windowAggStream =
        dataStream.filter(data -> "pv".equals(data.getBehaviorTpye()))//过滤用户行为
                .keyBy("itemId") //按商品id分组
                .timeWindow(Time.minutes(5), Time.minutes(1))//开窗
                .allowedLateness(Time.hours(1))
                .sideOutputLateData(lateTag)
                .aggregate(new ItemCountAgg(), new WindowItemCountResult());


```

### 2.3 收集同一窗口数据，排序输出top n

```java
public class TopNHotItems extends KeyedProcessFunction<Tuple, ItemViewCount, String> {
    private int topNum;
    private ListState<ItemViewCount> listState;
    private StringBuffer stringBuffer = new StringBuffer();
    public TopNHotItems(int i) {
        topNum = i;
    }

    @Override
    public void processElement(ItemViewCount value, Context ctx, Collector<String> out) throws Exception {

        listState.add(value);
        ctx.timerService().registerEventTimeTimer(value.getWindowEnd() + 1);

    }

    @Override
    public void open(Configuration parameters) throws Exception {
        listState = getRuntimeContext().getListState(new ListStateDescriptor<ItemViewCount>("item" +
                "-view-count-list", ItemViewCount.class));

    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {

        //定时器触发，已收集到所有数据，排序输出
        ArrayList<ItemViewCount> itemViewCounts = Lists.newArrayList(listState.get().iterator());

        itemViewCounts.sort(new Comparator<ItemViewCount>() {
            @Override
            public int compare(ItemViewCount o1, ItemViewCount o2) {
                return o2.getCount().intValue() - o1.getCount().intValue();
            }
        });


        Instant instant = Instant.ofEpochMilli(timestamp-1);
        ZoneId zone = ZoneId.systemDefault();
        LocalDateTime dateTime = LocalDateTime.ofInstant(instant, zone);
        
        //排名信息格式化成String
        stringBuffer.append("=========================\n");

        stringBuffer.append("窗口结束时间：").append(dateTime).append("\n");

        //便利列表，取top n输出

        for (int i = 0; i < Math.min(topNum, itemViewCounts.size()); i++) {
            ItemViewCount itemViewCount = itemViewCounts.get(i);

            stringBuffer.append("NO.").append(i + 1).append(":")
                    .append("  商品ID = ").append(itemViewCount.getItemId())
                    .append("  热门度 = ").append(itemViewCount.getCount())
                    .append("\n");

        }
        stringBuffer.append("=========================\n\n");

        Thread.sleep(1000L);
        out.collect(stringBuffer.toString());
    }
}

```

