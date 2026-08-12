package com.example.flink;

public class Trade {
    public String account;
    public String instrument;
    public String side;      
    public double qty;
    public double price;
    public long eventTime;   

    public Trade() {}        
    public Trade(String account, String instrument, String side,
                 double qty, double price, long eventTime) {
        this.account = account; this.instrument = instrument; this.side = side;
        this.qty = qty; this.price = price; this.eventTime = eventTime;
    }

    public double signedQty() { return "BUY".equals(side) ? qty : -qty; }

    @Override
    public String toString() {
        return account + "|" + instrument + "|" + side;
    }
}