package com.example.flink;

import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.*;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.util.Collector;

public class LimitBroadcastFn
    extends KeyedBroadcastProcessFunction<String, Trade, LimitRule, String> {

    private final MapStateDescriptor<String, Double> limitsDesc;
    private final long sustainedMs;

    private transient ValueState<Double> exposure;
    private transient ValueState<Long> timer;

    public LimitBroadcastFn(MapStateDescriptor<String, Double> limitsDesc, long sustainedMs) {
        this.limitsDesc = limitsDesc;
        this.sustainedMs = sustainedMs;
    }

    @Override
    public void open(OpenContext ctx) {
        exposure = getRuntimeContext().getState(new ValueStateDescriptor<>("exposure", Double.class));
        timer    = getRuntimeContext().getState(new ValueStateDescriptor<>("timer", Long.class));
    }

    @Override
    public void processBroadcastElement(LimitRule rule, Context ctx, Collector<String> out) throws Exception {
        ctx.getBroadcastState(limitsDesc).put(rule.account, rule.limit);
        out.collect("LIMIT-UPDATE " + rule.account + " -> " + rule.limit);
    }

    @Override
    public void processElement(Trade t, ReadOnlyContext ctx, Collector<String> out) throws Exception {
        double current = (exposure.value() == null ? 0.0 : exposure.value()) + t.signedQty();
        exposure.update(current);

        Double limit = ctx.getBroadcastState(limitsDesc).get(ctx.getCurrentKey());
        double effectiveLimit = (limit == null) ? 500.0 : limit;   // default until a rule arrives
        boolean over = Math.abs(current) > effectiveLimit;

        Long armed = timer.value();

        if (over && armed == null) {
            long fireAt = ctx.timestamp() + sustainedMs;
            ctx.timerService().registerEventTimeTimer(fireAt);
            timer.update(fireAt);
        } else if (!over && armed != null) {
            ctx.timerService().deleteEventTimeTimer(armed);
            timer.clear();
        }
    }

    @Override
    public void onTimer(long ts, OnTimerContext ctx, Collector<String> out) throws Exception {
        out.collect("ALERT " + ctx.getCurrentKey()
            + " sustained breach, exposure=" + exposure.value());
        timer.clear();
    }
}