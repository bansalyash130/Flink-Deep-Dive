package com.example.flink;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.source.util.ratelimit.RateLimiterStrategy;
import org.apache.flink.configuration.ExternalizedCheckpointRetention;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.connector.datagen.source.GeneratorFunction;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.util.OutputTag;
import java.time.Duration;
import java.util.concurrent.ThreadLocalRandom;

public class FlinkJob {

    static DataGeneratorSource<Trade> makeGenerator() {
        return new DataGeneratorSource<>(
            (GeneratorFunction<Long, Trade>) index -> randomTrade(),
            Long.MAX_VALUE,
            RateLimiterStrategy.perSecond(20),
            TypeInformation.of(Trade.class));
    }

    static Trade randomTrade() {
        var rnd = ThreadLocalRandom.current();
        String[] accts = {"ACC-1", "ACC-2", "ACC-3"};
        String[] instrs = {"AAPL", "MSFT", "GOOG", "AMZN"};
        long lagMs = ThreadLocalRandom.current().nextInt(0, 10_000);
        return new Trade(
            accts[rnd.nextInt(accts.length)],
            instrs[rnd.nextInt(instrs.length)],
            rnd.nextBoolean() ? "BUY" : "SELL",
            rnd.nextInt(1, 1000),
            50 + rnd.nextDouble() * 500,
            System.currentTimeMillis() - lagMs);
    }

    static DataGeneratorSource<LimitRule> limitUpdates() {
        return new DataGeneratorSource<>(
            (GeneratorFunction<Long, LimitRule>) i -> {
                String[] accts = {"ACC-1", "ACC-2", "ACC-3"};
                var rnd = ThreadLocalRandom.current();
                return new LimitRule(accts[rnd.nextInt(3)], 500 + rnd.nextInt(1000));
            },
            Long.MAX_VALUE,
            RateLimiterStrategy.perSecond(1),
            TypeInformation.of(LimitRule.class));
    }

    /** The entire streaming job. Blocks in env.execute() until cancelled. */
    public static void run() throws Exception {
        Configuration conf = new Configuration();
        conf.set(RestOptions.PORT, 8081);
        conf.setString("execution.backend.type", "rocksdb");
        conf.setString("execution.backend.incremental", "true");
        conf.setString("state.checkpoints.dir",
            "file:///C:/Users/bansa/IdeaProjects/Flink-Deep-Dive/checkpoints");
        conf.setString("restart-strategy.type", "fixed-delay");
        conf.setString("restart-strategy.fixed-delay.attempts", "10");
        conf.setString("restart-strategy.fixed-delay.delay", "3 s");

        var env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);
        env.enableCheckpointing(5000);
        env.getCheckpointConfig().setExternalizedCheckpointRetention(
            ExternalizedCheckpointRetention.RETAIN_ON_CANCELLATION);
        env.setParallelism(2);

        DataStream<LimitRule> limitUpdates = env.fromSource(
            limitUpdates(), WatermarkStrategy.noWatermarks(), "limit-updates");

        DataStream<Trade> trades = env.fromSource(
            makeGenerator(),
            WatermarkStrategy.<Trade>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                .withIdleness(Duration.ofSeconds(30))
                .withTimestampAssigner((element, ts) -> element.eventTime),
            "trades");

        OutputTag<Trade> sideOutput = new OutputTag<Trade>("late-trades") {};

        MapStateDescriptor<String, Double> limitsDesc =
            new MapStateDescriptor<>("limits", String.class, Double.class);
        BroadcastStream<LimitRule> limitsBroadcast = limitUpdates.broadcast(limitsDesc);

        DataStream<String> alerts = trades
            .keyBy(t -> t.account)
            .connect(limitsBroadcast)
            .process(new LimitBroadcastFn(limitsDesc, 1000))
            .uid("limit-broadcast-fn");
        alerts.print();

        DataStream<String> positions = trades
            .keyBy(t -> t.account + "|" + t.instrument)
            .process(new PositionFn())
            .uid("position-fn");

        DataStream<String> vwap = trades
            .keyBy(t -> t.account + "|" + t.instrument)
            .window(TumblingEventTimeWindows.of(Duration.ofSeconds(5)))
            .aggregate(new VwapAgg())
            .uid("vwap-agg");

        SingleOutputStreamOperator<Long> windowedTrades = trades
            .keyBy(t -> t.account + "|" + t.instrument)
            .window(TumblingEventTimeWindows.of(Duration.ofSeconds(5)))
            .allowedLateness(Duration.ofSeconds(2))
            .sideOutputLateData(sideOutput)
            .aggregate(new CountAgg());

        env.execute();
    }
}