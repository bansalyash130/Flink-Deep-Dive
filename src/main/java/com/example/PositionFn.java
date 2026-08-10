package com.example;

import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

public class PositionFn extends KeyedProcessFunction<String, Trade, String> {

    private transient ValueState<Double> currentPosition;

    @Override
    public void open(OpenContext ctx){
        currentPosition = getRuntimeContext().getState(new ValueStateDescriptor<>("net_position", Double.class));
    }

    @Override
    public void processElement(Trade value, KeyedProcessFunction<String, Trade, String>.Context ctx, Collector<String> out) throws Exception {
        Double current = currentPosition.value();
        if(current == null) current = 0.0;
        double signed = value.side.equals("BUY") ? value.qty : -value.qty;
        double updated = current + signed;

        currentPosition.update(updated);
        out.collect(ctx.getCurrentKey() + " position=" + updated);
    }
}