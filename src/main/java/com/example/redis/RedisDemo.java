package com.example.redis;

import com.example.flink.Trade;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.resps.Tuple;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;

public class RedisDemo {

    /** Generates trades and sinks running positions into Redis Hashes. Independent of Flink. */
    public static void run() {
        try (JedisPool pool = new JedisPool("localhost", 6379)) {
            run(pool, 100_000);
        } catch (Exception e) {
            System.err.println("[redis] error (is Memurai running on 6379?): " + e.getMessage());
        }
    }

    /**
     * Writes {@code iterations} synthetic trades through the given pool. Extracted from
     * {@link #run()} so tests can drive it with a mocked {@link JedisPool} and a small
     * iteration count. The pool's lifecycle is owned by the caller.
     */
    static void run(JedisPool pool, long iterations) {
        System.out.println("[redis] connected, writing positions...");

        for (long i = 0; i < iterations; i++) {
            Trade t = randomTrade();
            String key = "pos:" + t.account + "|" + t.instrument;

            try (Jedis j = pool.getResource()) {
                //hash
                j.hincrByFloat(key, "qty", t.signedQty());
                j.hincrByFloat(key, "notional", t.signedQty() * t.price);
                j.hset(key, "updated", Long.toString(t.eventTime));

                //leaderboard
                double tradeExposure = Math.abs(t.signedQty()*t.price);
                j.zincrby("risk:exposure", tradeExposure, t.account);

                //per instrument rolling stats
                long minute = t.eventTime / 60000;
                String statKey = "stat:" + t.instrument + ":" + minute;
                double absQty = Math.abs(t.signedQty());
                j.hincrByFloat(statKey, "volume",   absQty);
                j.hincrByFloat(statKey, "notional", absQty * t.price);
                j.hincrBy(statKey, "count", 1);
                j.expire(statKey, 3600);
            }

            if (i % 5_000 == 0) {
                try (Jedis j = pool.getResource()) {
                    Map<String, String> p = j.hgetAll("pos:ACC-1|AAPL");
                    System.out.println("[redis] pos:ACC-1|AAPL -> " + p);

                    // top risks
                    System.out.println("[redis] --- top risk ---");
                    List<Tuple> top = j.zrevrangeWithScores("risk:exposure", 0,4);
                    for (Tuple t2 : top) {
                        System.out.printf("  %-6s exposure=%.2f%n", t2.getElement(), t2.getScore());
                    }

                    //stats data
                    long m = System.currentTimeMillis() / 60_000;
                    String sk = "stat:AAPL:" + m;
                    Map<String, String> s = j.hgetAll(sk);
                    if (!s.isEmpty()) {
                        double vol = Double.parseDouble(s.getOrDefault("volume", "0"));
                        double not = Double.parseDouble(s.getOrDefault("notional", "0"));
                        double vwap = vol == 0 ? 0 : not / vol;
                        long ttl = j.ttl(sk);
                        System.out.printf("[redis] AAPL this minute: vol=%.0f vwap=%.2f count=%s ttl=%ds%n",
                            vol, vwap, s.getOrDefault("count", "0"), ttl);
                    }
                }
            }

            try { Thread.sleep(2); } catch (InterruptedException e) {
                Thread.currentThread().interrupt(); break;
            }
        }
        System.out.println("[redis] done.");
    }

    static Trade randomTrade() {
        var rnd = ThreadLocalRandom.current();
        String[] accts = {"ACC-1", "ACC-2", "ACC-3"};
        String[] instrs = {"AAPL", "MSFT", "GOOG", "AMZN"};
        return new Trade(
            accts[rnd.nextInt(3)],
            instrs[rnd.nextInt(4)],
            rnd.nextBoolean() ? "BUY" : "SELL",
            rnd.nextInt(1, 1000),
            50 + rnd.nextDouble() * 500,
            System.currentTimeMillis());
    }
}