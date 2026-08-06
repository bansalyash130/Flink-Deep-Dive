package com.example;

import org.apache.flink.api.common.functions.AggregateFunction;

// AggregateFunction<IN, ACC, OUT>
//   IN  = Trade            (each record folded in)
//   ACC = double[]{sumPxQty, sumQty}   (the tiny running accumulator)
//   OUT = String           (the formatted result when the window fires)
public class VwapAgg implements AggregateFunction<Trade, double[], String> {

    @Override
    public double[] createAccumulator() {
        return new double[]{0.0, 0.0};      // [0] = Σ(price*qty), [1] = Σ(qty)
    }

    @Override
    public double[] add(Trade t, double[] acc) {
        acc[0] += t.price * t.qty;          // fold each trade in as it arrives
        acc[1] += t.qty;
        return acc;
    }

    @Override
    public String getResult(double[] acc) {
        double vwap = acc[1] == 0 ? 0 : acc[0] / acc[1];   // VWAP = Σ(px*qty) / Σ(qty)
        return "vol=" + acc[1] + " vwap=" + String.format("%.2f", vwap);
    }

    @Override
    public double[] merge(double[] a, double[] b) {
        return new double[]{a[0] + b[0], a[1] + b[1]};     // used when windows merge (e.g. session)
    }
}