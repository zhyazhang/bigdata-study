package com.aifurion.hotitem;

import com.aifurion.beans.ItemViewCount;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * @author ：zzy
 * @description：TODO
 * @date ：2021/11/30 10:56
 */
public class WindowItemCountResult implements WindowFunction<Long, ItemViewCount, Tuple,TimeWindow> {
    @Override
    public void apply(Tuple tuple, TimeWindow window, Iterable<Long> input,
                      Collector<ItemViewCount> out) throws Exception {

        Long itemId = tuple.getField(0);
        Long windowEnd = window.getEnd();
        Long count = input.iterator().next();
        out.collect(new ItemViewCount(itemId, windowEnd, count));


    }
}
