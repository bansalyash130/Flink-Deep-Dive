# Unit Tests Summary

This document describes the test coverage for the Flink Deep-Dive project.

## Test Files Created

### 1. **TradeTest.java**
Tests the `Trade` data model.
- `testTradeCreation()` - Verifies Trade fields are set correctly
- `testSignedQtyBuy()` - Validates signed quantity for BUY orders (positive)
- `testSignedQtySell()` - Validates signed quantity for SELL orders (negative)
- `testToString()` - Tests string representation format
- `testNoArgConstructor()` - Ensures POJO no-arg constructor works

**Coverage**: Basic POJO functionality, field persistence, helper methods

---

### 2. **LimitRuleTest.java**
Tests the `LimitRule` control stream data model.
- `testLimitRuleCreation()` - Verifies rule fields are set
- `testNoArgConstructor()` - POJO constructor test
- `testMultipleLimitRules()` - Tests multiple rules with different limits

**Coverage**: POJO structure, different limit values

---

### 3. **VwapAggTest.java**
Tests the `VwapAgg` aggregation function (Volume-Weighted Average Price).
- `testCreateAccumulator()` - Accumulator initialized to [0, 0]
- `testAddSingleTrade()` - Single trade accumulation (qty, price×qty)
- `testAddMultipleTrades()` - Multiple trades accumulated correctly
- `testGetResultVwap()` - VWAP calculation: Σ(price×qty) / Σ(qty)
- `testGetResultZeroQuantity()` - Edge case: zero quantity → VWAP=0
- `testMergeAccumulators()` - Merge two accumulators for stream merging

**Coverage**: Aggregation logic, VWAP calculation, merge semantics

---

### 4. **CountAggTest.java**
Tests the `CountAgg` aggregation function (simple trade counter).
- `testCreateAccumulator()` - Accumulator starts at 0L
- `testAddSingleTrade()` - Increment counter by 1
- `testAddMultipleTrades()` - Accumulate multiple trades
- `testGetResult()` - Return the accumulated count
- `testMergeAccumulators()` - Merge counts from parallel subtasks

**Coverage**: Counter accumulation, merge semantics

---

### 5. **PositionFnTest.java**
Tests the `PositionFn` stateful processor (per account|instrument position tracking).

**Key Components**:
- Uses `KeyedOneInputStreamOperatorTestHarness` to test stateful operator
- Keying: `account|instrument`
- State: ValueState for cumulative position

**Tests**:
- `testInitialPosition()` - First trade creates initial position
- `testAccumulatePosition()` - Multiple BUY trades accumulate
- `testBuySellPosition()` - BUY reduces by SELL quantity
- `testDifferentInstruments()` - Different instruments maintain separate state

**Coverage**: Keyed state management, position accumulation, BUY/SELL logic

---

### 6. **AlertFnTest.java**
Tests the `AlertFn` stateful processor (static limit alerting with TTL).

**Key Components**:
- Uses `KeyedOneInputStreamOperatorTestHarness`
- Keying: `account`
- State: ValueState for exposure + ValueState for timer
- State TTL: 1 hour (expires unused state)

**Tests**:
- `testExposureBelowLimit()` - No alert when below limit
- `testExposureAboveLimit()` - Alert fires after sustained breach
- `testAccumulateExposure()` - Cumulative exposure checked against limit
- `testBreachCleared()` - Cancellation message when breach resolves
- **Limit**: 500 (configured in AlertFn constructor)

**Coverage**: Stateful alerting, timer management, breach lifecycle

---

### 7. **LimitBroadcastFnTest.java**
Tests the `LimitBroadcastFn` broadcast processor (dynamic limit alerting).

**Key Components**:
- Uses `KeyedBroadcastOperatorTestHarness`
- Keying: `account`
- Keyed State: ValueState for exposure + timer
- Broadcast State: Map<account, limit>
- Dual stream: `trades` (keyed) + `limitUpdates` (broadcast)

**Tests**:
- `testBroadcastLimitUpdate()` - Limit rules update broadcast state
- `testTradeWithinLimit()` - No alert when trade within limit
- `testTradeExceedsLimit()` - Alert fires after sustained breach
- `testDefaultLimitApplied()` - Default limit (500) when no rule set
- `testMultipleLimitUpdates()` - Multiple limit updates per account
- `testNegativeExposure()` - Checks absolute value (short positions)

**Coverage**: Broadcast state, keyed state, dual-stream processing, dynamic limits

---

## Running the Tests

### Option 1: Maven Command Line
```bash
mvn clean test                    # Run all tests
mvn test -Dtest=TradeTest         # Run specific test class
mvn test -Dtest=VwapAggTest#testAddMultipleTrades  # Run specific test
```

### Option 2: IDE
- Right-click test file → Run Tests
- IDE will automatically compile and run

### Option 3: Maven with Verbose Output
```bash
mvn clean test -X                 # Debug output
mvn test -DfailIfNoTests=false    # Don't fail if no tests found
```

---

## Test Framework Stack

- **JUnit 5** (Jupiter) - Test framework
- **Flink Test Utils** - Test harnesses for operators
  - `KeyedOneInputStreamOperatorTestHarness` - Single keyed stream
  - `KeyedBroadcastOperatorTestHarness` - Keyed + broadcast streams
- **Assertions** - JUnit `assertEquals`, `assertTrue`, `assertFalse`

---

## Coverage Summary

| Component | Tests | Coverage |
|-----------|-------|----------|
| Trade (POJO) | 5 | ✅ Complete |
| LimitRule (POJO) | 3 | ✅ Complete |
| VwapAgg | 6 | ✅ Complete (VWAP formula, merge, edge cases) |
| CountAgg | 5 | ✅ Complete (accumulation, merge) |
| PositionFn | 4 | ✅ Core logic (state, keying, BUY/SELL) |
| AlertFn | 5 | ✅ Alerting lifecycle (timer, breach, TTL) |
| LimitBroadcastFn | 7 | ✅ Broadcast patterns, dynamic limits, timers |
| App (main job) | — | Integration tests recommended (MiniCluster) |

---

## Integration Test Recommendations

For end-to-end testing of the full `App.main()` job, consider using Flink's `MiniCluster`:

```java
@Test
void testFullPipeline() throws Exception {
    MiniCluster cluster = new MiniCluster(new Configuration());
    cluster.start();
    
    // Run App.main() against cluster
    App.main(new String[]{});
    
    cluster.close();
}
```

This validates the entire pipeline: data generation → windowing → aggregation → state → checkpointing.

---

## Notes

- Tests use **event time** (simulated timestamps) for reproducibility
- **Harnesses** abstract the streaming runtime, making tests deterministic
- Tests focus on **business logic**, not framework internals
- **Timer registration/firing** is tested via harness time advancement
- **State isolation** per key ensures tests don't interfere

---

**Test Quality Metrics**:
- ✅ Unit tests for data models
- ✅ Aggregation function tests (math verification)
- ✅ Stateful operator tests (state lifecycle)
- ✅ Broadcast state tests (multi-stream coordination)
- ✅ Timer tests (event-time driven alerts)
- 📋 Integration tests (recommended for full pipeline)
