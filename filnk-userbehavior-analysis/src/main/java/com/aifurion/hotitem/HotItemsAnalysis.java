package com.aifurion.hotitem;

import com.aifurion.beans.ItemViewCount;
import com.aifurion.beans.UserBehavior;
import com.aifurion.utils.KafkaUtil;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.OutputTag;

/**
 * @author ：zzy
 * @description：TODO 实时分析pv
 * @date ：2021/11/30 10:29
 */
public class HotItemsAnalysis {

    public static void main(String[] args) throws Exception {

        //1、创建执行环境

        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();

        //设置并行度
        env.setParallelism(4);

        //设置时间语义
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        //2、读取数据，创建DataStream
        //kafka数据源

        DataStream<String> inputStream = env.addSource(new FlinkKafkaConsumer<String>("behavior"
                , new SimpleStringSchema(), KafkaUtil.getProperties()));

        //3、转换为pojo，分配时间戳和watermaker

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


        //4、分组开窗聚合，得到每个窗口内各个商品的count值


        //定义侧输出流标签
        OutputTag<UserBehavior> lateTag = new OutputTag<UserBehavior>("late") {
        };

        SingleOutputStreamOperator<ItemViewCount> windowAggStream =
                dataStream.filter(data -> "pv".equals(data.getBehaviorTpye()))//过滤用户行为
                        .keyBy("itemId") //按商品id分组
                        .timeWindow(Time.minutes(5), Time.minutes(1))//开窗
                        .allowedLateness(Time.hours(1))
                        .sideOutputLateData(lateTag)
                        .aggregate(new ItemCountAgg(), new WindowItemCountResult());


        //5、收集同一窗口的所有商品count数据，排序输出top n

        //获得侧输出流
        //windowAggStream.getSideOutput(lateTag).print("late");


        SingleOutputStreamOperator<String> resultStream = windowAggStream
                .keyBy("windowEnd") //按窗口分组
                .process(new TopNHotItems(10));//自定义处理函数，排序top n


        resultStream.print();

        env.execute("hot item analysis");
    }

}
