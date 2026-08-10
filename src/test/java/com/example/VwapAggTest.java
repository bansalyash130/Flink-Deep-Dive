package com.example;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class VwapAggTest {

    @Test
    void testCreateAccumulator() {
        VwapAgg agg = new VwapAgg();
        double[] acc = agg.createAccumulator();
        
        assertEquals(0.0, acc[0]);
        assertEquals(0.0, acc[1]);
    }

    @Test
    void testAddSingleTrade() {
        VwapAgg agg = new VwapAgg();
        double[] acc = agg.createAccumulator();
        
        Trade trade = new Trade("ACC-1", "AAPL", "BUY", 100, 150.0, System.currentTimeMillis());
        acc = agg.add(trade, acc);
        
        assertEquals(15000.0, acc[0]); // 150 * 100
        assertEquals(100.0, acc[1]);   // qty
    }

    @Test
    void testAddMultipleTrades() {
        VwapAgg agg = new VwapAgg();
        double[] acc = agg.createAccumulator();
        
        Trade trade1 = new Trade("ACC-1", "AAPL", "BUY", 100, 100.0, System.currentTimeMillis());
        Trade trade2 = new Trade("ACC-1", "AAPL", "BUY", 200, 200.0, System.currentTimeMillis());
        
        acc = agg.add(trade1, acc);
        acc = agg.add(trade2, acc);
        
        // sumPxQty = 100*100 + 200*200 = 10000 + 40000 = 50000
        // sumQty = 100 + 200 = 300
        assertEquals(50000.0, acc[0]);
        assertEquals(300.0, acc[1]);
    }

    @Test
    void testGetResultVwap() {
        VwapAgg agg = new VwapAgg();
        double[] acc = new double[]{50000.0, 300.0}; // VWAP should be 166.67
        
        String result = agg.getResult(acc);
        
        assertTrue(result.contains("volume=300.0"));
        assertTrue(result.contains("vwap=166.67"));
    }

    @Test
    void testGetResultZeroQuantity() {
        VwapAgg agg = new VwapAgg();
        double[] acc = new double[]{0.0, 0.0};
        
        String result = agg.getResult(acc);
        
        assertTrue(result.contains("volume=0.0"));
        assertTrue(result.contains("vwap=0.00"));
    }

    @Test
    void testMergeAccumulators() {
        VwapAgg agg = new VwapAgg();
        double[] acc1 = new double[]{10000.0, 100.0};
        double[] acc2 = new double[]{20000.0, 200.0};
        
        double[] merged = agg.merge(acc1, acc2);
        
        assertEquals(30000.0, merged[0]);
        assertEquals(300.0, merged[1]);
    }
}
