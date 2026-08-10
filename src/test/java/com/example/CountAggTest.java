package com.example;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class CountAggTest {

    @Test
    void testCreateAccumulator() {
        CountAgg agg = new CountAgg();
        Long count = agg.createAccumulator();
        
        assertEquals(0L, count);
    }

    @Test
    void testAddSingleTrade() {
        CountAgg agg = new CountAgg();
        Long count = agg.createAccumulator();
        
        Trade trade = new Trade("ACC-1", "AAPL", "BUY", 100, 150.0, System.currentTimeMillis());
        count = agg.add(trade, count);
        
        assertEquals(1L, count);
    }

    @Test
    void testAddMultipleTrades() {
        CountAgg agg = new CountAgg();
        Long count = agg.createAccumulator();
        
        for (int i = 0; i < 5; i++) {
            Trade trade = new Trade("ACC-1", "AAPL", "BUY", 100, 150.0, System.currentTimeMillis());
            count = agg.add(trade, count);
        }
        
        assertEquals(5L, count);
    }

    @Test
    void testGetResult() {
        CountAgg agg = new CountAgg();
        Long count = 42L;
        
        Long result = agg.getResult(count);
        
        assertEquals(42L, result);
    }

    @Test
    void testMergeAccumulators() {
        CountAgg agg = new CountAgg();
        
        Long merged = agg.merge(100L, 50L);
        
        assertEquals(150L, merged);
    }
}
