package com.example.flink;

import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.time.Duration;

public class AlertFn extends KeyedProcessFunction<String,Trade , String> {

    private transient ValueState<Double> exposure;
    private transient ValueState<Long> timer;

    private final Double limit;
    private final Long sustainedMs;

    public AlertFn(Double limit, Long sustainedMs) {
        this.limit = limit;
        this.sustainedMs = sustainedMs;
    }

    @Override
    public void open(OpenContext ctx){

        StateTtlConfig ttl = StateTtlConfig
            .newBuilder(Duration.ofHours(1))
            .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
            .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
            .build();

        ValueStateDescriptor<Double> expDesc = new ValueStateDescriptor<>("exposure", Double.class);
        expDesc.enableTimeToLive(ttl);
        exposure = getRuntimeContext().getState(expDesc);
        timer = getRuntimeContext().getState(new ValueStateDescriptor<>("timer", Long.class));

    }

    @Override
    public void processElement(Trade value, KeyedProcessFunction<String, Trade, String>.Context ctx, Collector<String> out) throws Exception {

        double current = (exposure.value() == null? 0.0: exposure.value()) + value.signedQty();
        exposure.update(current);

        boolean over = current > limit;
        Long armed = timer.value();

        if(over && armed == null){
            long fireAt = ctx.timestamp() + sustainedMs;
            ctx.timerService().registerEventTimeTimer(fireAt);
            timer.update(fireAt);
        }else if(!over && armed != null){
            ctx.timerService().deleteEventTimeTimer(armed);
            timer.clear();
            out.collect("CANCELLED " + ctx.getCurrentKey() + " breach cleared before firing");
        }
    }

    @Override
    public void onTimer(long timestamp, KeyedProcessFunction<String, Trade, String>.OnTimerContext ctx, Collector<String> out) throws Exception {
        out.collect("ALERT " + ctx.getCurrentKey()
            + " sustained breach, exposure=" + exposure.value()
            + " (limit=" + limit + ")");
        timer.clear();
    }
}
