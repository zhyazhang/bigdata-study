package com.aifurion.trading;

import com.aifurion.beans.TotalBuyCount;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * @author ：zzy
 * @description：TODO
 * @date ：2021/11/30 16:47
 */
public class TotalTradingValume extends KeyedProcessFunction<Long, TotalBuyCount, TotalBuyCount> {

    private ValueState<Long> totalValue;

    @Override
    public void open(Configuration parameters) throws Exception {
        totalValue = getRuntimeContext().getState(new ValueStateDescriptor<Long>("total-buy-count"
                , Long.class, 0L));
    }

    @Override
    public void processElement(TotalBuyCount value, Context ctx, Collector<TotalBuyCount> out) throws Exception {
        totalValue.update(totalValue.value() + value.getCount());

        ctx.timerService().registerEventTimeTimer(value.getWindowEnd() + 1);

    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<TotalBuyCount> out) throws Exception {
        Long count = totalValue.value();

        out.collect(new TotalBuyCount("buy", ctx.getCurrentKey(), count));
        totalValue.clear();

    }
}
