package com.aifurion.hotitem;

import com.aifurion.beans.ItemViewCount;
import org.apache.commons.compress.utils.Lists;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Comparator;

/**
 * @author ：zzy
 * @description：TODO
 * @date ：2021/11/30 10:56
 */
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
