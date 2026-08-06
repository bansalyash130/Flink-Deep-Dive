package com.example;

import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

public class PositionFn extends KeyedProcessFunction<String, Trade, String> {
    private transient ValueState<Double> netQty;      

    public void open(OpenContext ctx) {                
        netQty = getRuntimeContext().getState(
            new ValueStateDescriptor<>("netQty", Double.class));
    }

    public void processElement(Trade t, Context ctx, Collector<String> out) throws Exception {
        double cur = netQty.value() == null ? 0.0 : netQty.value(); 
        double updated = cur + t.signedQty();
        netQty.update(updated);                              
        out.collect(ctx.getCurrentKey() + " position=" + updated);
    }
}