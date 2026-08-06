
package com.example;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.source.util.ratelimit.RateLimiterStrategy;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.connector.datagen.source.GeneratorFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.util.OutputTag;
import java.time.Duration;
import java.util.concurrent.ThreadLocalRandom;

public class App {

     static DataGeneratorSource<Trade> makeGenerator() {
        return new DataGeneratorSource<>(
            (GeneratorFunction<Long, Trade>) index -> randomTrade(),
            Long.MAX_VALUE,
            RateLimiterStrategy.perSecond(50),
            TypeInformation.of(Trade.class));
    }

    static Trade randomTrade() {
        var rnd = ThreadLocalRandom.current();
        String[] accts  = {"ACC-1", "ACC-2", "ACC-3"};
        String[] instrs = {"AAPL", "MSFT", "GOOG", "AMZN"};
        long lagMs = ThreadLocalRandom.current().nextInt(0, 10_000);   // 0–10s old 
        return new Trade(
            accts[rnd.nextInt(accts.length)],
            instrs[rnd.nextInt(instrs.length)],
            rnd.nextBoolean() ? "BUY" : "SELL",
            rnd.nextInt(1, 1000),
            50 + rnd.nextDouble() * 500,
            System.currentTimeMillis() - lagMs);
    }

    public static void main(String[] args) throws Exception {
        
        Configuration conf = new Configuration();
        conf.set(RestOptions.PORT, 8081);   // optional — 8081 is the default anyway
        var env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);
        env.setParallelism(2);

        DataStream<Trade> trades =
            env.fromSource(makeGenerator(), 
            WatermarkStrategy.<Trade>forBoundedOutOfOrderness(Duration.ofSeconds(5))
            .withTimestampAssigner((t, ts) -> t.eventTime)
            .withIdleness(Duration.ofSeconds(10))
            , "trades");

        // trades
        //     .filter(t -> t.qty * t.price > 100_000)              // keep large-notional trades
        //     .map(t -> t.account + " " + t.side + " " + t.instrument
        //              + " notional=" + (t.qty * t.price))
        //     .print();

        // trades.process(new ProcessFunction<Trade, String>() {
        //     @Override
        //     public void processElement(Trade t, Context ctx, Collector<String> out) {
        //         out.collect("trade@" + t.eventTime
        //             + "  watermark=" + ctx.timerService().currentWatermark());
        //     }
        // }).print();

    //    trades.process(new ProcessFunction<Trade, String>() {
    //         @Override
    //         public void processElement(Trade t, Context ctx, Collector<String> out) {
    //             long now = ctx.timerService().currentProcessingTime();   // Flink's processing-time clock
    //             long lag = now - t.eventTime;                            // ms between event and processing
    //             out.collect(t.account + "|" + t.instrument
    //                 + " eventTime=" + t.eventTime
    //                 + " procTime=" + now
    //                 + " lagMs=" + lag);
    //         }
    //     }).print();

        // DataStream<String> positions = trades
        // .keyBy(t -> t.account + "|" + t.instrument)
        // .process(new PositionFn())
        // .uid("position-fn");                 
        // positions.print();

        OutputTag<Trade> lateTag = new OutputTag<Trade>("late-trades") {};

        SingleOutputStreamOperator<String> windowed = trades
            .keyBy(t -> t.instrument)
            .window(TumblingEventTimeWindows.of(Duration.ofSeconds(10)))
            .allowedLateness(Duration.ofSeconds(5))
            .sideOutputLateData(lateTag)                     // divert late records here instead of dropping silently
            .aggregate(new VwapAgg());

        // the on-time window results
        windowed.print();

        // TODO: handle late records using process functions
        windowed.getSideOutput(lateTag)
            .map(t -> "LATE  " + t.instrument
                + "  lateBy≈" + (System.currentTimeMillis() - t.eventTime) + "ms")
            .print();

        env.execute("step1-first-job");                         
    }

   
}