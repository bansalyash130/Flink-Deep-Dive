package com.example;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class LimitRuleTest {

    @Test
    void testLimitRuleCreation() {
        LimitRule rule = new LimitRule("ACC-1", 5000.0);
        
        assertEquals("ACC-1", rule.account);
        assertEquals(5000.0, rule.limit);
    }

    @Test
    void testNoArgConstructor() {
        LimitRule rule = new LimitRule();
        assertNotNull(rule);
    }

    @Test
    void testMultipleLimitRules() {
        LimitRule rule1 = new LimitRule("ACC-1", 1000.0);
        LimitRule rule2 = new LimitRule("ACC-2", 2000.0);
        LimitRule rule3 = new LimitRule("ACC-3", 3000.0);
        
        assertEquals(1000.0, rule1.limit);
        assertEquals(2000.0, rule2.limit);
        assertEquals(3000.0, rule3.limit);
    }
}
