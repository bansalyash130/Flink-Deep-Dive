package com.example;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.operators.KeyedProcessOperator;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.concurrent.ConcurrentLinkedQueue;

import static org.junit.jupiter.api.Assertions.*;

class PositionFnTest {

    private KeyedOneInputStreamOperatorTestHarness<String, Trade, String> harness;

    @BeforeEach
    void setup() throws Exception {
        PositionFn positionFn = new PositionFn();
        harness = new KeyedOneInputStreamOperatorTestHarness<>(
            new KeyedProcessOperator<>(positionFn),
            t -> t.account + "|" + t.instrument,
            TypeInformation.of(String.class)
        );
        harness.open();
    }

    @Test
    void testInitialPosition() throws Exception {
        Trade buyTrade = new Trade("ACC-1", "AAPL", "BUY", 100, 150.0, System.currentTimeMillis());
        
        harness.processElement(buyTrade, System.currentTimeMillis());
        
        ConcurrentLinkedQueue<Object> output = harness.getOutput();
        assertEquals(1, output.size());
        assertTrue(output.peek().toString().contains("ACC-1|AAPL position=100.0"));
    }

    @Test
    void testAccumulatePosition() throws Exception {
        Trade trade1 = new Trade("ACC-1", "AAPL", "BUY", 100, 150.0, System.currentTimeMillis());
        Trade trade2 = new Trade("ACC-1", "AAPL", "BUY", 50, 150.0, System.currentTimeMillis());
        
        harness.processElement(trade1, System.currentTimeMillis());
        harness.processElement(trade2, System.currentTimeMillis());
        
        ConcurrentLinkedQueue<Object> output = harness.getOutput();
        assertEquals(2, output.size());
    }

    @Test
    void testBuySellPosition() throws Exception {
        Trade buyTrade = new Trade("ACC-1", "AAPL", "BUY", 100, 150.0, System.currentTimeMillis());
        Trade sellTrade = new Trade("ACC-1", "AAPL", "SELL", 30, 150.0, System.currentTimeMillis());
        
        harness.processElement(buyTrade, System.currentTimeMillis());
        harness.processElement(sellTrade, System.currentTimeMillis());
        
        ConcurrentLinkedQueue<Object> output = harness.getOutput();
        assertEquals(2, output.size());
    }

    @Test
    void testDifferentInstruments() throws Exception {
        Trade tradeAApl = new Trade("ACC-1", "AAPL", "BUY", 100, 150.0, System.currentTimeMillis());
        Trade tradeMsft = new Trade("ACC-1", "MSFT", "BUY", 50, 300.0, System.currentTimeMillis());
        
        harness.processElement(tradeAApl, System.currentTimeMillis());
        harness.processElement(tradeMsft, System.currentTimeMillis());
        
        ConcurrentLinkedQueue<Object> output = harness.getOutput();
        assertEquals(2, output.size());
    }
}
