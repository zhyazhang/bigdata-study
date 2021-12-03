package com.aifurion.trading;

import com.aifurion.beans.TotalBuyCount;
import com.aifurion.beans.UserBehavior;
import com.aifurion.utils.KafkaUtil;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Random;

/**
 * @author ：zzy
 * @description：TODO 实时分析购买量
 * @date ：2021/11/30 16:27
 */
public class TradingVolumeAnalysis {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();


        env.setParallelism(4);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);


        DataStreamSource<String> inputDataStream = env.addSource(new FlinkKafkaConsumer<String>(
                "behavior", new SimpleStringSchema(),
                KafkaUtil.getProperties()));

        SingleOutputStreamOperator<UserBehavior> dataStream =
                inputDataStream.map(line -> {

                            System.out.println(line);
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


        DataStream<TotalBuyCount> tradingAgg =
                dataStream.filter(data -> "buy".equals(data.getBehaviorTpye()))
                        .map(new MapFunction<UserBehavior, Tuple2<Integer, Long>>() {
                            @Override
                            public Tuple2<Integer, Long> map(UserBehavior value) throws Exception {
                                Random random = new Random();
                                return new Tuple2<>(random.nextInt(), 1L);
                            }
                        })
                        .keyBy(data -> data.f0)
                        .timeWindow(Time.minutes(2))
                        .aggregate(new TradingCountAgg(), new TradingCountResult());

        DataStream<TotalBuyCount> resultStream = tradingAgg
                .keyBy(TotalBuyCount::getWindowEnd)
                .process(new TotalTradingValume());


        resultStream.print();
        env.execute("traing volume count");
    }
}
