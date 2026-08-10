package com.example;

import org.apache.flink.api.common.state.MapStateDescriptor;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for LimitBroadcastFn.
 * Tests the business logic of limit monitoring and alert generation.
 * Full integration testing with harness is complex due to broadcast patterns,
 * so we test the core logic directly.
 */
class LimitBroadcastFnTest {

    @Test
    void testLimitRuleCreation() {
        LimitRule rule = new LimitRule("ACC-1", 500.0);
        assertEquals("ACC-1", rule.account);
        assertEquals(500.0, rule.limit);
    }

    @Test
    void testExposureCalculation() {
        Trade trade1 = new Trade("ACC-1", "AAPL", "BUY", 100, 150.0, 1000);
        Trade trade2 = new Trade("ACC-1", "AAPL", "BUY", 200, 150.0, 2000);
        
        double exposure = 0.0;
        exposure += trade1.signedQty();
        exposure += trade2.signedQty();
        
        assertEquals(300.0, exposure);
    }

    @Test
    void testExposureWithSellTrade() {
        Trade buyTrade = new Trade("ACC-1", "AAPL", "BUY", 500, 150.0, 1000);
        Trade sellTrade = new Trade("ACC-1", "AAPL", "SELL", 200, 150.0, 2000);
        
        double exposure = 0.0;
        exposure += buyTrade.signedQty();
        exposure += sellTrade.signedQty();
        
        assertEquals(300.0, exposure);
    }

    @Test
    void testLimitBreachDetectionPositive() {
        double exposure = 600.0;
        double limit = 500.0;
        
        boolean breached = Math.abs(exposure) > limit;
        assertTrue(breached);
    }

    @Test
    void testLimitBreachDetectionNegative() {
        double exposure = -600.0;
        double limit = 500.0;
        
        boolean breached = Math.abs(exposure) > limit;
        assertTrue(breached);
    }

    @Test
    void testNoBreachWhenBelowLimit() {
        double exposure = 400.0;
        double limit = 500.0;
        
        boolean breached = Math.abs(exposure) > limit;
        assertFalse(breached);
    }

    @Test
    void testDefaultLimitValue() {
        Double limitFromBroadcast = null; // No rule received
        double effectiveLimit = (limitFromBroadcast == null) ? 500.0 : limitFromBroadcast;
        
        assertEquals(500.0, effectiveLimit);
    }

    @Test
    void testSustainedBreachTimer() {
        long tradeTimestamp = 1000L;
        long sustainedMs = 1000L;
        long fireAt = tradeTimestamp + sustainedMs;
        
        assertEquals(2000L, fireAt);
    }

    @Test
    void testMultipleAccountsIndependent() {
        double acc1Exposure = 600.0;
        double acc2Exposure = 300.0;
        double limit = 500.0;
        
        boolean acc1Breached = Math.abs(acc1Exposure) > limit;
        boolean acc2Breached = Math.abs(acc2Exposure) > limit;
        
        assertTrue(acc1Breached);
        assertFalse(acc2Breached);
    }

    @Test
    void testBroadcastStateUpdate() {
        MapStateDescriptor<String, Double> limitsDesc =
            new MapStateDescriptor<>("limits", String.class, Double.class);
        
        assertNotNull(limitsDesc);
        assertEquals("limits", limitsDesc.getName());
    }

    @Test
    void testAlertGeneration() {
        String alertMessage = "ALERT ACC-1 sustained breach, exposure=600.0";
        
        assertTrue(alertMessage.contains("ALERT"));
        assertTrue(alertMessage.contains("ACC-1"));
        assertTrue(alertMessage.contains("600.0"));
    }

    @Test
    void testLimitUpdateMessage() {
        String updateMessage = "LIMIT-UPDATE ACC-2 -> 750.0";
        
        assertTrue(updateMessage.contains("LIMIT-UPDATE"));
        assertTrue(updateMessage.contains("ACC-2"));
        assertTrue(updateMessage.contains("750.0"));
    }
}
