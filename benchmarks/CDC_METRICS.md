<!-- Copyright (c) 2026 Gembit Soultan Shirazi <gembit.soultan@gmail.com>. All rights reserved. -->

# CDC Benchmarking Metrics for Gpool

This document defines the key performance indicators (KPIs) for evaluating the Change Data Capture (CDC) capabilities of Gpool against existing solutions like `go-pq-cdc`.

## 1. End-to-End Latency (P50, P90, P99)
- **Definition**: The time elapsed from the `COMMIT` of a transaction on the PostgreSQL database to the moment the change event is received by the application-level handler in Gpool.
- **Measurement**: 
    - Insert a record with a high-resolution timestamp.
    - Calculate the difference upon receipt.
- **Target**: Sub-millisecond latency for local deployments.

## 2. Throughput (Events per Second)
- **Definition**: The maximum number of change events Gpool can process and deliver per second.
- **Measurement**: 
    - Perform a large batch of operations (e.g., 100k INSERTS) in a single transaction or multiple parallel transactions.
    - Measure the time taken to receive all events.
- **Target**: >50,000 events/sec.

## 3. LSN Lag (Bytes)
- **Definition**: The distance between the current WAL write location (`pg_current_wal_lsn()`) and the last LSN confirmed by the replication slot.
- **Measurement**: 
    - Query `pg_replication_slots` for `confirmed_flush_lsn`.
    - Monitor lag during high-load periods to ensure it doesn't grow indefinitely.

## 4. Message Parsing Time (Nanoseconds per Event)
- **Definition**: Time spent by the library decoding the `pgoutput` binary stream into structured Go entities.
- **Measurement**: 
    - Use Go micro-benchmarks on the parsing logic.
- **Strategy**: Leverage `sync.Pool` to achieve zero-allocation parsing in the hot path.

## 5. CPU & Memory Overhead
- **Definition**: Resource consumption of the CDC subscriber relative to the event volume.
- **Measurement**: 
    - `pprof` analysis during throughput tests.
    - Allocations per event (`testing.B.ReportAllocs`).
