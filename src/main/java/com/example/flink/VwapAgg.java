package com.example.flink;

import org.apache.flink.api.common.functions.AggregateFunction;

// AggregateFunction<IN, ACC, OUT>
//   IN  = Trade            (each record folded in)
//   ACC = double[]{sumPxQty, sumQty}   (the tiny running accumulator)
//   OUT = String           (the formatted result when the window fires)
public class VwapAgg implements AggregateFunction<Trade, double[], String> {

    @Override
    public double[] createAccumulator() {
        return new double[]{0,0};
    }

    @Override
    public double[] add(Trade value, double[] accumulator) {

       accumulator[0] = accumulator[0] + value.price*value.qty;
       accumulator[1] = accumulator[1] + value.qty;
       return accumulator;
    }

    @Override
    public String getResult(double[] accumulator) {
        double sumPxQty = accumulator[0];
        double sumQty   = accumulator[1];
        double vwap = (sumQty == 0) ? 0 : sumPxQty / sumQty;
        return "volume=" + sumQty + " vwap=" + String.format("%.2f", vwap);
    }

    @Override
    public double[] merge(double[] a, double[] b) {
        a[0] += b[0];
        a[1] += b[1];
        return a;
    }
}