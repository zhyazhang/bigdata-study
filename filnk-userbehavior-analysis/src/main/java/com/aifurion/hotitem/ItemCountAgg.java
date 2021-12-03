package com.aifurion.hotitem;

import com.aifurion.beans.UserBehavior;
import org.apache.flink.api.common.functions.AggregateFunction;

/**
 * @author ：zzy
 * @description：TODO
 * @date ：2021/11/30 10:55
 */
public class ItemCountAgg implements AggregateFunction<UserBehavior, Long, Long> {
    @Override
    public Long createAccumulator() {
        return 0L;
    }

    @Override
    public Long add(UserBehavior value, Long accumulator) {
        return accumulator + 1;
    }

    @Override
    public Long getResult(Long accumulator) {
        return accumulator;
    }

    @Override
    public Long merge(Long a, Long b) {
        return a + b;
    }
}
