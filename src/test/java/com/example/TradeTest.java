package com.example;

import com.example.flink.Trade;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class TradeTest {

    @Test
    void testTradeCreation() {
        Trade trade = new Trade("ACC-1", "AAPL", "BUY", 100, 150.0, System.currentTimeMillis());
        
        assertEquals("ACC-1", trade.account);
        assertEquals("AAPL", trade.instrument);
        assertEquals("BUY", trade.side);
        assertEquals(100, trade.qty);
        assertEquals(150.0, trade.price);
    }

    @Test
    void testSignedQtyBuy() {
        Trade buyTrade = new Trade("ACC-1", "AAPL", "BUY", 100, 150.0, System.currentTimeMillis());
        assertEquals(100.0, buyTrade.signedQty());
    }

    @Test
    void testSignedQtySell() {
        Trade sellTrade = new Trade("ACC-1", "AAPL", "SELL", 50, 150.0, System.currentTimeMillis());
        assertEquals(-50.0, sellTrade.signedQty());
    }

    @Test
    void testToString() {
        Trade trade = new Trade("ACC-2", "MSFT", "BUY", 200, 300.0, System.currentTimeMillis());
        assertEquals("ACC-2|MSFT|BUY", trade.toString());
    }

    @Test
    void testNoArgConstructor() {
        Trade trade = new Trade();
        assertNotNull(trade);
    }
}
