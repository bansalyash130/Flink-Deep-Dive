package com.example;

public class LimitRule {
    public String account;
    public double limit;
    public LimitRule() {}                       // no-arg ctor for POJO
    public LimitRule(String account, double limit) {
        this.account = account; this.limit = limit;
    }
}