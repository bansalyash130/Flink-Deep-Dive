package com.example;

import com.example.flink.LimitBroadcastFn;
import com.example.flink.LimitRule;
import com.example.flink.Trade;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.operators.co.CoBroadcastWithKeyedOperator;
import org.apache.flink.streaming.util.KeyedBroadcastOperatorTestHarness;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.concurrent.ConcurrentLinkedQueue;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Drives the real {@link LimitBroadcastFn} operator through Flink's
 * {@link KeyedBroadcastOperatorTestHarness}, exercising broadcast state,
 * keyed state, event-time timers and the sustained-breach alert lifecycle.
 */
class LimitBroadcastFnTest {

    private static final long SUSTAINED_MS = 1000L;

    private MapStateDescriptor<String, Double> limitsDesc;
    private KeyedBroadcastOperatorTestHarness<String, Trade, LimitRule, String> harness;

    @BeforeEach
    void setup() throws Exception {
        limitsDesc = new MapStateDescriptor<>("limits", String.class, Double.class);
        LimitBroadcastFn fn = new LimitBroadcastFn(limitsDesc, SUSTAINED_MS);

        CoBroadcastWithKeyedOperator<String, Trade, LimitRule, String> operator =
            new CoBroadcastWithKeyedOperator<>(fn, Collections.singletonList(limitsDesc));

        harness = new KeyedBroadcastOperatorTestHarness<>(
            operator,
            (Trade t) -> t.account,
            TypeInformation.of(String.class),
            1, 1, 0);
        harness.open();
    }

    @AfterEach
    void teardown() throws Exception {
        harness.close();
    }

    /**
     * Advances event time on BOTH inputs. A two-input operator's watermark is
     * the minimum across its inputs, so the broadcast side must advance too or
     * the combined watermark never reaches the timer.
     */
    private void advanceWatermark(long ts) throws Exception {
        harness.processWatermark(ts);
        harness.processBroadcastWatermark(ts);
    }

    /** Text of all output records collected so far. */
    private String outputText() {
        StringBuilder sb = new StringBuilder();
        for (Object o : harness.getOutput()) {
            sb.append(o.toString()).append('\n');
        }
        return sb.toString();
    }

    @Test
    void testBroadcastLimitUpdate() throws Exception {
        harness.processBroadcastElement(new LimitRule("ACC-1", 5000.0), 1000);

        assertTrue(outputText().contains("LIMIT-UPDATE ACC-1 -> 5000.0"),
            "Broadcasting a rule should emit a LIMIT-UPDATE message");
        assertEquals(5000.0, harness.getBroadcastState(limitsDesc).get("ACC-1"),
            "Broadcast state should hold the updated limit");
    }

    @Test
    void testTradeWithinLimit() throws Exception {
        harness.processBroadcastElement(new LimitRule("ACC-1", 5000.0), 1000);

        // exposure 100 << limit 5000 -> no timer, no alert even after time passes
        harness.processElement(new Trade("ACC-1", "AAPL", "BUY", 100, 150.0, 1000), 1000);
        advanceWatermark(10_000);

        assertFalse(outputText().contains("ALERT"), "No alert expected within limit");
    }

    @Test
    void testSustainedBreachFiresAlert() throws Exception {
        harness.processBroadcastElement(new LimitRule("ACC-1", 500.0), 1000);

        // exposure 600 > limit 500 -> arms timer at 1000 + SUSTAINED_MS
        harness.processElement(new Trade("ACC-1", "AAPL", "BUY", 600, 150.0, 1000), 1000);
        advanceWatermark(1000 + SUSTAINED_MS + 1);

        String out = outputText();
        assertTrue(out.contains("ALERT ACC-1 sustained breach"),
            "Sustained breach should fire an ALERT");
        assertTrue(out.contains("exposure=600.0"), "Alert should report the exposure");
    }

    @Test
    void testDefaultLimitAppliedWhenNoRule() throws Exception {
        // No broadcast rule -> default limit of 500 applies. 700 > 500 -> alert.
        harness.processElement(new Trade("ACC-2", "MSFT", "BUY", 700, 150.0, 1000), 1000);
        advanceWatermark(1000 + SUSTAINED_MS + 1);

        assertTrue(outputText().contains("ALERT ACC-2 sustained breach"),
            "Default limit (500) should be applied when no rule has arrived");
    }

    @Test
    void testBreachClearedCancelsTimer() throws Exception {
        harness.processBroadcastElement(new LimitRule("ACC-3", 500.0), 1000);

        // exposure 600 > 500 -> arms timer
        harness.processElement(new Trade("ACC-3", "AAPL", "BUY", 600, 150.0, 1000), 1000);
        // SELL 300 -> exposure 300 < 500 -> cancels timer
        harness.processElement(new Trade("ACC-3", "AAPL", "SELL", 300, 150.0, 1500), 1500);
        advanceWatermark(10_000);

        assertFalse(outputText().contains("ALERT"),
            "Cleared breach should cancel the timer before it fires");
    }

    @Test
    void testNegativeExposureBreaches() throws Exception {
        harness.processBroadcastElement(new LimitRule("ACC-1", 500.0), 1000);

        // Net short: -600, abs value 600 > 500 -> breach
        harness.processElement(new Trade("ACC-1", "AAPL", "SELL", 600, 150.0, 1000), 1000);
        advanceWatermark(1000 + SUSTAINED_MS + 1);

        String out = outputText();
        assertTrue(out.contains("ALERT ACC-1 sustained breach"),
            "Absolute exposure should breach on short positions");
        assertTrue(out.contains("exposure=-600.0"), "Alert should report signed exposure");
    }

    @Test
    void testSustainedBreachKeepsExistingTimer() throws Exception {
        harness.processBroadcastElement(new LimitRule("ACC-1", 500.0), 1000);

        // First breach arms the timer (over && armed == null).
        harness.processElement(new Trade("ACC-1", "AAPL", "BUY", 600, 150.0, 1000), 1000);
        // Second breach while still armed -> no-op, keeps the original timer (over && armed != null).
        harness.processElement(new Trade("ACC-1", "AAPL", "BUY", 100, 150.0, 1500), 1500);
        // Original timer fires at 1000 + SUSTAINED_MS; exposure is now 700.
        advanceWatermark(1000 + SUSTAINED_MS + 1);

        String out = outputText();
        assertTrue(out.contains("ALERT ACC-1 sustained breach"),
            "Sustained breach should still fire from the original timer");
        assertTrue(out.contains("exposure=700.0"), "Alert should reflect accumulated exposure");
        assertEquals(1, outputText().lines().filter(l -> l.contains("ALERT")).count(),
            "Only one alert should fire for a single sustained breach");
    }

    @Test
    void testMultipleLimitUpdatesLastWins() throws Exception {
        harness.processBroadcastElement(new LimitRule("ACC-1", 500.0), 1000);
        harness.processBroadcastElement(new LimitRule("ACC-1", 5000.0), 1100);

        assertEquals(5000.0, harness.getBroadcastState(limitsDesc).get("ACC-1"),
            "Later limit update should overwrite the earlier one");

        // Under the new higher limit, 600 no longer breaches.
        harness.processElement(new Trade("ACC-1", "AAPL", "BUY", 600, 150.0, 1200), 1200);
        advanceWatermark(10_000);
        assertFalse(outputText().contains("ALERT"),
            "Raised limit should prevent the alert");
    }

    @Test
    void testAccountsAreIndependent() throws Exception {
        harness.processBroadcastElement(new LimitRule("ACC-1", 500.0), 1000);
        harness.processBroadcastElement(new LimitRule("ACC-2", 500.0), 1000);

        harness.processElement(new Trade("ACC-1", "AAPL", "BUY", 600, 150.0, 1000), 1000); // breach
        harness.processElement(new Trade("ACC-2", "MSFT", "BUY", 100, 150.0, 1000), 1000); // fine
        advanceWatermark(1000 + SUSTAINED_MS + 1);

        ConcurrentLinkedQueue<Object> out = harness.getOutput();
        long alerts = out.stream().filter(o -> o.toString().contains("ALERT")).count();
        assertEquals(1, alerts, "Only the breaching account should alert");
        assertTrue(outputText().contains("ALERT ACC-1"), "ACC-1 should be the one alerting");
    }
}
