package com.example.redis;

import com.example.flink.Trade;
import org.junit.jupiter.api.Test;
import org.mockito.MockedConstruction;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

/**
 * Unit tests for {@link RedisDemo}. The Redis IO is mocked with Mockito, so no live
 * server is required and the write/read-back logic runs against a fake {@link Jedis}.
 */
class RedisDemoTest {

    private static final List<String> ACCOUNTS = Arrays.asList("ACC-1", "ACC-2", "ACC-3");
    private static final List<String> INSTRUMENTS = Arrays.asList("AAPL", "MSFT", "GOOG", "AMZN");

    @Test
    void testRandomTradeProducesValidFields() {
        long before = System.currentTimeMillis();
        for (int i = 0; i < 1000; i++) {
            Trade t = RedisDemo.randomTrade();

            assertTrue(ACCOUNTS.contains(t.account), "unexpected account: " + t.account);
            assertTrue(INSTRUMENTS.contains(t.instrument), "unexpected instrument: " + t.instrument);
            assertTrue(t.side.equals("BUY") || t.side.equals("SELL"), "unexpected side: " + t.side);
            assertTrue(t.qty >= 1 && t.qty <= 999, "qty out of range: " + t.qty);
            assertTrue(t.price >= 50.0 && t.price <= 550.0, "price out of range: " + t.price);
            assertTrue(t.eventTime >= before && t.eventTime <= System.currentTimeMillis(),
                "eventTime out of range: " + t.eventTime);
        }
    }

    @Test
    void testRunWritesPositionAndReadsBack() {
        JedisPool pool = mock(JedisPool.class);
        Jedis jedis = mock(Jedis.class);
        when(pool.getResource()).thenReturn(jedis);
        when(jedis.hgetAll("pos:ACC-1|AAPL")).thenReturn(Map.of("qty", "5"));

        RedisDemo.run(pool, 1); // i=0 => write + read-back (0 % 5000 == 0)

        // Each trade updates qty, notional and the 'updated' timestamp atomically.
        verify(jedis).hincrByFloat(startsWith("pos:"), eq("qty"), anyDouble());
        verify(jedis).hincrByFloat(startsWith("pos:"), eq("notional"), anyDouble());
        verify(jedis).hset(startsWith("pos:"), eq("updated"), anyString());
        // i=0 triggers the periodic read-back.
        verify(jedis).hgetAll("pos:ACC-1|AAPL");
    }

    @Test
    void testReadBackOnlyOnEvery5000th() {
        JedisPool pool = mock(JedisPool.class);
        Jedis jedis = mock(Jedis.class);
        when(pool.getResource()).thenReturn(jedis);
        when(jedis.hgetAll(anyString())).thenReturn(Map.of());

        RedisDemo.run(pool, 2); // i=0 reads back, i=1 does not

        verify(jedis, times(2)).hincrByFloat(anyString(), eq("qty"), anyDouble());
        verify(jedis, times(1)).hgetAll("pos:ACC-1|AAPL");
    }

    @Test
    void testRunStopsOnInterrupt() {
        JedisPool pool = mock(JedisPool.class);
        Jedis jedis = mock(Jedis.class);
        when(pool.getResource()).thenReturn(jedis);
        when(jedis.hgetAll(anyString())).thenReturn(Map.of());

        Thread.currentThread().interrupt(); // Thread.sleep(2) will throw and break the loop
        RedisDemo.run(pool, 5);

        // Loop breaks after the first iteration's write, before completing all 5.
        verify(jedis, times(1)).hincrByFloat(anyString(), eq("qty"), anyDouble());
        assertTrue(Thread.interrupted(), "interrupt flag should be restored (and is now cleared)");
    }

    @Test
    void testNoArgRunHandlesConnectionFailure() {
        // Intercept `new JedisPool(...)` so no real server is contacted; make getResource fail.
        try (MockedConstruction<JedisPool> mc = mockConstruction(JedisPool.class,
                (mockPool, ctx) -> when(mockPool.getResource())
                    .thenThrow(new RuntimeException("connection refused")))) {

            RedisDemo.run(); // failure is caught and logged internally, not propagated
            assertEquals(1, mc.constructed().size(), "run() should construct exactly one pool");
        }
    }
}
