package com.aifurion.trading;

import com.aifurion.beans.TotalBuyCount;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * @author ：zzy
 * @description：TODO
 * @date ：2021/11/30 16:47
 */
public class TradingCountResult implements WindowFunction<Long, TotalBuyCount, Integer, TimeWindow> {
    @Override
    public void apply(Integer integer, TimeWindow window, Iterable<Long> input,
                      Collector<TotalBuyCount> out) throws Exception {


    }
}
