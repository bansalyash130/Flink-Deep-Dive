package com.example;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.operators.KeyedProcessOperator;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.concurrent.ConcurrentLinkedQueue;

import static org.junit.jupiter.api.Assertions.*;

class AlertFnTest {

    private KeyedOneInputStreamOperatorTestHarness<String, Trade, String> harness;

    @BeforeEach
    void setup() throws Exception {
        AlertFn alertFn = new AlertFn(500.0, 1000L);
        harness = new KeyedOneInputStreamOperatorTestHarness<>(
            new KeyedProcessOperator<>(alertFn),
            t -> t.account,
            TypeInformation.of(String.class)
        );
        harness.open();
    }

    @Test
    void testExposureBelowLimit() throws Exception {
        Trade trade = new Trade("ACC-1", "AAPL", "BUY", 100, 150.0, 1000);
        
        harness.processElement(trade, 1000);
        
        ConcurrentLinkedQueue<Object> output = harness.getOutput();
        // No alert should be generated - exposure (100) < limit (500)
        assertEquals(0, output.size());
    }

    @Test
    void testExposureAboveLimit() throws Exception {
        // Limit is 500, single trade with qty 600 should trigger alert
        Trade trade = new Trade("ACC-1", "AAPL", "BUY", 600, 150.0, 1000);
        
        harness.processElement(trade, 1000);
        // Advance event time to fire the timer (registered at 1000 + 1000 = 2000)
        harness.processWatermark(2001);
        
        ConcurrentLinkedQueue<Object> output = harness.getOutput();
        // Timer should fire after 1000ms
        assertTrue(output.size() > 0, "Expected alert when exposure (600) > limit (500)");
    }

    @Test
    void testAccumulateExposure() throws Exception {
        Trade trade1 = new Trade("ACC-1", "AAPL", "BUY", 300, 150.0, 1000);
        Trade trade2 = new Trade("ACC-1", "AAPL", "BUY", 300, 150.0, 2000);
        
        harness.processElement(trade1, 1000);  // exposure = 300 (within limit)
        harness.processElement(trade2, 2000);  // exposure = 600 (exceeds 500 limit), timer set to fire at 3000
        
        // Timer won't fire until we advance to 3000+
        harness.processWatermark(3100);
        
        ConcurrentLinkedQueue<Object> output = harness.getOutput();
        assertTrue(output.size() > 0, "Expected alert when cumulative exposure (600) > limit (500)");
    }

    @Test
    void testBreachCleared() throws Exception {
        Trade buyTrade = new Trade("ACC-1", "AAPL", "BUY", 600, 150.0, 1000);
        Trade sellTrade = new Trade("ACC-1", "AAPL", "SELL", 200, 150.0, 2000);
        
        harness.processElement(buyTrade, 1000);  // exposure = 600 (exceeds limit)
        harness.processElement(sellTrade, 2000); // exposure = 400 (within limit)
        
        ConcurrentLinkedQueue<Object> output = harness.getOutput();
        // Should contain cancellation message since breach was cleared
        boolean hasCancellation = output.stream()
            .anyMatch(o -> o.toString().contains("CANCELLED"));
        assertTrue(hasCancellation, "Expected CANCELLED message when breach clears");
    }
}
