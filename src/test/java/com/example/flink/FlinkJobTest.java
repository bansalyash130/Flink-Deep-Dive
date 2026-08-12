package com.example.flink;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for the pure helpers in {@link FlinkJob}. The pipeline itself ({@code run()})
 * runs an unbounded source and blocks in {@code env.execute()}, so it is out of scope for
 * unit testing and would require a bounded MiniCluster integration test instead.
 */
class FlinkJobTest {

    private static final List<String> ACCOUNTS = Arrays.asList("ACC-1", "ACC-2", "ACC-3");
    private static final List<String> INSTRUMENTS = Arrays.asList("AAPL", "MSFT", "GOOG", "AMZN");

    @Test
    void testRandomTradeProducesValidFields() {
        long before = System.currentTimeMillis();
        for (int i = 0; i < 1000; i++) {
            Trade t = FlinkJob.randomTrade();

            assertTrue(ACCOUNTS.contains(t.account), "unexpected account: " + t.account);
            assertTrue(INSTRUMENTS.contains(t.instrument), "unexpected instrument: " + t.instrument);
            assertTrue(t.side.equals("BUY") || t.side.equals("SELL"), "unexpected side: " + t.side);
            assertTrue(t.qty >= 1 && t.qty <= 999, "qty out of range: " + t.qty);
            assertTrue(t.price >= 50.0 && t.price <= 550.0, "price out of range: " + t.price);
            // Trades are stamped 0-10s in the past to simulate lag.
            long now = System.currentTimeMillis();
            assertTrue(t.eventTime <= now && t.eventTime >= before - 10_000,
                "eventTime out of expected lag window: " + t.eventTime);
        }
    }

    @Test
    void testMakeGeneratorIsConstructed() {
        assertNotNull(FlinkJob.makeGenerator(), "trade generator source should be constructed");
    }

    @Test
    void testLimitUpdatesSourceIsConstructed() {
        assertNotNull(FlinkJob.limitUpdates(), "limit-updates source should be constructed");
    }
}
