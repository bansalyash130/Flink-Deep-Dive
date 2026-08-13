# Flink Deep-Dive: Real-Time Financial Trading Analytics

A hands-on Apache Flink project demonstrating core streaming concepts through a realistic financial trading pipeline. Generates synthetic trade data and computes real-time analytics including position tracking, VWAP (Volume-Weighted Average Price), and trade counting with late-data handling.

## Project Overview

This project explores Apache Flink 2.2.0 streaming capabilities with a practical use case: analyzing financial trades in real-time. The application:

- **Generates synthetic trades** at 20 trades/sec across 3 accounts and 4 instruments (AAPL, MSFT, GOOG, AMZN)
- **Tracks account positions** per instrument using stateful processing
- **Computes VWAP** in 5-second tumbling windows
- **Handles late data** with 2-second grace periods and side outputs
- **Manages state** with RocksDB backend and incremental checkpointing
- **Recovers from failures** with fixed-delay restart strategy (10 attempts, 3s delay)

It also ships a small **standalone Redis demo** (`RedisDemo`) that sinks running positions into
Redis hashes, independent of Flink. The `App` entry point selects what to run via a mode argument
(`both` | `flink` | `redis`, default `both`).

### Code layout

- `com.example.App` — entry point / mode dispatcher
- `com.example.flink.*` — the Flink streaming job and its functions (`FlinkJob`, `Trade`, `LimitRule`,
  `LimitBroadcastFn`, `PositionFn`, `VwapAgg`, `CountAgg`, `AlertFn`)
- `com.example.redis.RedisDemo` — Flink-independent position sink into Redis

## Architecture

### Components

#### 1. **Trade Data Model** (`Trade.java`)
- **Fields**: account, instrument, side (BUY/SELL), quantity, price, eventTime
- **Realistic Lag**: Trades timestamped 0–10 seconds in the past (simulates network latency)
- **Helper**: `signedQty()` converts signed quantities for position tracking

#### 2. **Trade Generator** (`FlinkJob.makeGenerator()`)
- Synthetic data source using Flink's `DataGeneratorSource`
- Generates trades indefinitely at 20 trades/sec
- Random selection from:
  - Accounts: ACC-1, ACC-2, ACC-3
  - Instruments: AAPL, MSFT, GOOG, AMZN
  - Sides: BUY or SELL
  - Quantities: 1–999 units
  - Prices: $50–$550

#### 3. **Limit Rules & Risk Control** (`LimitRule.java`)
- Data model for dynamic exposure limits per account
- **Fields**: account, limit (max exposure threshold)
- Broadcasted from slow control stream (1 rule/sec)
- Simulates risk desk updating position limits in real-time

#### 4. **Limit Broadcast Processor** (`LimitBroadcastFn.java`)
- `KeyedBroadcastProcessFunction<String, Trade, LimitRule, String>`
- **Broadcast State**: Map of account → exposure limit
- **Keyed State**: Per-account exposure + timer for sustained breaches
- **Logic**:
  - Updates exposure on each trade (signed quantity)
  - Reads current limit from broadcast state
  - Triggers event-time timer if exposure exceeds limit
  - Cancels timer if exposure drops back under limit
  - Fires alert if breach is sustained for the configured window (`sustainedMs`, wired to 1s in `FlinkJob`)
  - Default limit of 500 applies per account until a rule arrives on the broadcast stream
- **Outputs**:
  - `LIMIT-UPDATE ACC-1 -> 5000` (limit changed)
  - `ALERT ACC-2 sustained breach, exposure=5500.0` (threshold violated)

#### 5. **Position Tracking** (`PositionFn.java`)
- `KeyedProcessFunction` with stateful computation
- Maintains net position per account|instrument key
- Updates cumulative position on each trade (BUY adds, SELL subtracts)
- Outputs: `ACC-1|AAPL position=150.0`

#### 6. **VWAP Aggregation** (`VwapAgg.java`)
- Tumbling event-time windows (5-second intervals)
- Accumulator: `[sumPriceQty, sumQty]`
- Formula: VWAP = Σ(price × qty) / Σ(qty)
- Outputs: `volume=3500.0 vwap=275.45`

#### 7. **Trade Counting** (`CountAgg.java`)
- Counts trades per window
- Simple aggregator: accumulates trade count

#### 8. **Alert Function** (`AlertFn.java`) *(Optional/Standalone)*
- Simpler version of limit monitoring: fixed limit per account
- Demonstrates state TTL (expires exposure after 1 hour of inactivity)
- Alternative to broadcast-based dynamic limits

### Streaming Pipeline

```
Limit Rules (1/sec)
    ↓ broadcast
    |
Trade Generator (20/sec)          
    ↓
Event Time Watermarking (5s out-of-order, 30s idle)
    ↓
    ├─────────────────────────────────────────────┐
    │                                             │
    │  Keyby: account + connect to broadcast     │
    │        (dynamic limit updates)             │
    │        ↓                                   │
    │   LimitBroadcastFn                        │
    │   (stateful exposure tracking + timers)   │
    │   → ALERTS on sustained breach            │
    │                                             │
    ├────────────────────────────────────────────┤
    │                                             │
    │  Keyby: account|instrument                 │
    │   ├─→ PositionFn (stateful)               │
    │   │   └─→ Position updates                │
    │   │                                        │
    │   ├─→ VWAP Window (5s tumbling)           │
    │   │   └─→ VWAP metrics                    │
    │   │                                        │
    │   └─→ Count Window (5s + 2s late grace)   │
    │       ├─→ Trade counts                    │
    │       └─→ Side output: late trades        │
    │                                             │
    └──────────────────────────────────────────────┘
              ↓
          Console Output
```

## Redis Demo (standalone)

`com.example.redis.RedisDemo` is a small, Flink-independent sink that generates the same synthetic
trades and writes several running views into Redis (via Jedis). It runs in the `redis` (or default
`both`) mode, connects to `localhost:6379`, and logs a connection error and exits cleanly if no
server is reachable.

Keys written per trade:

| Key | Type | Contents |
|-----|------|----------|
| `pos:<account>\|<instrument>` | Hash | Running position: `qty`, `notional` (both `HINCRBYFLOAT`), `updated` (event time) |
| `risk:exposure` | Sorted Set | Per-account exposure leaderboard: `ZINCRBY` by `\|signedQty x price\|` |
| `stat:<instrument>:<minute>` | Hash | Per-instrument, per-minute rolling stats: `volume`, `notional`, `count`; 1h TTL via `EXPIRE 3600` |

Every 5,000 trades it reads a few views back and prints them:
- `pos:ACC-1|AAPL` current position (`HGETALL`)
- Top-5 risk leaderboard (`ZREVRANGE ... WITHSCORES`)
- Current-minute AAPL stats with VWAP (`notional / volume`) and remaining TTL

All Redis IO is unit-tested with a mocked `JedisPool`/`Jedis` (no live server needed) — see
[TEST_SUMMARY.md](TEST_SUMMARY.md).

## Configuration

### State Management
- **Backend**: RocksDB (persistent, incremental)
- **Incremental Checkpointing**: Enabled (only changed state is saved)
- **Checkpoint Interval**: 5 seconds
- **Retention**: Externalized checkpoints retained on cancellation

### Fault Tolerance
- **Restart Strategy**: Fixed-delay (10 attempts, 3-second delay)
- **Checkpoint Directory**: currently hardcoded in `FlinkJob.run()` to an absolute Windows path
  (`file:///C:/Users/bansa/IdeaProjects/Flink-Deep-Dive/checkpoints`) — change this before running elsewhere

### Execution
- **Parallelism**: 2
- **Environment**: Local with Web UI (http://localhost:8081)
- **Port**: 8081

## Running the Project

### Prerequisites
- Java 17+
- Maven 3.8+
- Flink 2.2.0 (embedded via Maven)
- (Optional) A Redis-compatible server on `localhost:6379` (e.g. Memurai/Redis) for the `redis` mode

### Build
```bash
mvn clean package
```

### Run

`App` takes an optional mode argument: `both` (default) | `flink` | `redis`.

```bash
# Flink job + Redis demo (default)
java -jar target/flink-deep-dive-1.0-SNAPSHOT.jar

# Flink streaming job only
java -jar target/flink-deep-dive-1.0-SNAPSHOT.jar flink

# Redis position sink only
java -jar target/flink-deep-dive-1.0-SNAPSHOT.jar redis
```

Or run directly in IDE (Main class: `com.example.App`). The Redis demo logs a connection error and
exits cleanly if no server is reachable, so `both` still works without Redis installed.

### Monitor
- Web Dashboard: http://localhost:8081
- Watch console output for:
  - Position updates: `ACC-1|AAPL position=150.0`
  - VWAP results: `volume=3500.0 vwap=275.45`
  - Trade counts per window

## Key Flink Concepts Demonstrated

| Concept | Implementation |
|---------|----------------|
| **Event Time Processing** | Timestamps from Trade, WatermarkStrategy for late data |
| **Windowing** | Tumbling event-time windows (5s) with late arrival grace period |
| **Keyed State** | PositionFn & LimitBroadcastFn maintain per-key state (position, exposure, timers) |
| **Broadcast State** | LimitBroadcastFn reads dynamic limits from broadcast control stream |
| **State TTL** | AlertFn demonstrates expiring state after 1 hour of inactivity |
| **Event-Time Timers** | LimitBroadcastFn fires alerts after a sustained breach (configurable timer, 1s in `FlinkJob`) |
| **Stream Connectivity** | `connect()` + `KeyedBroadcastProcessFunction` joins trades with limit updates |
| **Stateless Aggregation** | AggregateFunction for VWAP and count computation |
| **Side Outputs** | Late trades captured in separate stream |
| **Checkpointing** | RocksDB backend, incremental snapshots, 5s interval |
| **Watermarks** | Trades: 5s out-of-order tolerance + 30s idleness. Broadcast limit stream is marked idle so it does not pin the connected operator's event time (see [Watermarks in the connected operator](#watermarks-in-the-connected-operator)) |

## Output Examples

```
LIMIT-UPDATE ACC-1 -> 5000
volume=3500.0 vwap=275.45
LIMIT-UPDATE ACC-2 -> 7500
ACC-2|MSFT position=450.0
volume=2100.0 vwap=198.34
ACC-1|GOOG position=-120.0
ALERT ACC-1 sustained breach, exposure=5200.0
volume=4200.0 vwap=320.12
LIMIT-UPDATE ACC-3 -> 4500
```

## Project Structure

```
Flink-Deep-Dive/
├── src/main/java/com/example/
│   ├── App.java                     # Entry point / mode dispatcher (both | flink | redis)
│   ├── flink/
│   │   ├── FlinkJob.java            # The Flink streaming job (pipeline wiring, run())
│   │   ├── Trade.java              # Trade data model
│   │   ├── LimitRule.java          # Control stream: dynamic exposure limits
│   │   ├── LimitBroadcastFn.java   # Broadcast processor: risk monitoring + alerts
│   │   ├── PositionFn.java         # Stateful position processor (per account|instrument)
│   │   ├── AlertFn.java            # Static limit alert (standalone, not wired into FlinkJob)
│   │   ├── VwapAgg.java            # VWAP aggregation function
│   │   └── CountAgg.java           # Trade count aggregation function
│   └── redis/
│       └── RedisDemo.java          # Flink-independent position sink into Redis
├── src/test/java/com/example/       # JUnit 5 tests (+ Flink harnesses, Mockito) — see TEST_SUMMARY.md
├── pom.xml                          # Maven config (Flink 2.2.0, RocksDB, Kafka connector, Jedis, Mockito)
├── TEST_SUMMARY.md                  # Test coverage documentation
└── ReadMe.md                       # This file
```

## Next Steps / Future Enhancements

- [ ] Kafka integration for real data ingestion
- [ ] Time-series metrics storage (InfluxDB/Prometheus)
- [ ] Advanced window strategies (sliding, session windows)
- [ ] Multiple state backends comparison
- [ ] Distributed cluster deployment
- [ ] Alerting on position thresholds
- [ ] Custom metrics and monitoring

## Key Highlights

### Broadcast State Pattern
- **Control stream** (limit updates) broadcasts to all parallel instances
- Each keyed task sees the entire limits map
- Dynamic reconfiguration without redeploying the job
- Example: Risk desk updates account limits in real-time

### Timers for Sustained Alerting
- Timer fires only if exposure stays over the limit for the sustained window (1s as wired)
- Transient breaches don't trigger alerts
- Cancels timer automatically if exposure normalizes
- Uses event time for reproducible, deterministic behavior

### Watermarks in the connected operator
- The alert operator connects the keyed trade stream with the broadcast limit stream. A connected
  operator's event time is the **minimum** of its inputs' watermarks.
- Alerts are emitted from an **event-time timer** (`onTimer`), so the operator's event time must
  advance for any alert to fire.
- If the broadcast limit stream produces no watermarks (`WatermarkStrategy.noWatermarks()`), its
  watermark stays at `-inf` and pins the operator at `-inf` — the timer never fires and **no alerts
  are ever produced** (though `LIMIT-UPDATE` lines, emitted synchronously, still appear).
- Fix: the limit stream uses `forMonotonousTimestamps().withIdleness(1s)` so it is marked idle and
  excluded from the watermark minimum, letting the trade stream drive event time forward.

### State TTL (Optional)
- `AlertFn` demonstrates state expiration after 1 hour
- Prevents unbounded growth of state for dormant accounts
- Useful for cleanup in long-running jobs

## Notes

- Trades are generated with realistic lag (0–10s) to test watermark handling
- Limit rules trickle in at 1 per second (control stream is low-volume)
- RocksDB provides durability; checkpoints enable recovery from failures
- Restart strategy ensures automatic recovery without manual intervention
- Web UI provides visibility into topology, metrics, and checkpoint history
- Two approaches to alerting:
  - **LimitBroadcastFn**: Dynamic, per-account limits from control stream
  - **AlertFn**: Static limit passed to its constructor, simpler but less flexible (not wired into `FlinkJob`; standalone/tested in isolation)
