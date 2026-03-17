# Benchmarks

All benchmarks run with `redis-benchmark`, 50 clients, 1M requests per test. Sequential runs (one server at a time) on a 32-core Intel i9-14900K, 128GB RAM, Ubuntu 24.04.

Lux v0.5.0 vs Redis 8.6.1. Median of 10 runs for headline SET numbers; single-run for per-command breakdown.

## SET throughput by pipeline depth (median of 10 runs)

| Pipeline | Lux | Redis 8.6.1 | Lux/Redis |
|----------|-----|-------------|-----------|
| 1 | 264K | 286K | 0.92x |
| 16 | 3.44M | 2.07M | **1.66x** |
| 64 | **10.0M** | 2.71M | **3.69x** |
| 128 | **14.7M** | 2.86M | **5.14x** |
| 256 | **18.2M** | 2.99M | **6.09x** |
| 512 | **20.4M** | 2.98M | **6.85x** |

At pipeline=1 (no batching), Lux and Redis are within noise. The gap grows with pipeline depth because Lux batches same-shard commands under a single lock while Redis processes commands sequentially on one core.

## All commands at pipeline depth 64

| Command | Lux | Redis 8.6.1 | Lux/Redis |
|---------|-----|-------------|-----------|
| SET | 11.2M | 3.3M | **3.4x** |
| GET | 12.0M | 4.7M | **2.6x** |
| INCR | 6.3M | 4.0M | **1.6x** |
| LPUSH | 6.5M | 3.3M | **2.0x** |
| RPUSH | 6.4M | 3.7M | **1.7x** |
| LPOP | 11.6M | 3.0M | **3.9x** |
| RPOP | 11.1M | 3.3M | **3.4x** |
| SADD | 7.2M | 4.1M | **1.8x** |
| HSET | 6.8M | 3.3M | **2.0x** |
| SPOP | 12.2M | 4.5M | **2.7x** |
| ZADD | 7.0M | 3.1M | **2.3x** |
| ZPOPMIN | 11.5M | 5.3M | **2.2x** |
| PING | 12.0M | 7.3M | **1.6x** |
| LRANGE_100 | 461K | 422K | **1.1x** |
| LRANGE_300 | 132K | 129K | **1.0x** |
| MSET (10 keys) | 197K | 727K | 0.27x |

Lux beats Redis on every single-key command. MSET is slower because it touches multiple shards (requiring multiple locks); Redis has no lock overhead for multi-key operations.

## Pipeline depth 1 (no batching)

| Command | Lux | Redis 8.6.1 | Lux/Redis |
|---------|-----|-------------|-----------|
| SET | 291K | 255K | **1.14x** |
| GET | 292K | 257K | **1.14x** |
| INCR | 294K | 257K | **1.14x** |
| LPUSH | 294K | 257K | **1.14x** |
| RPUSH | 295K | 257K | **1.15x** |
| LPOP | 291K | 257K | **1.13x** |
| RPOP | 289K | 256K | **1.13x** |
| SADD | 288K | 283K | **1.02x** |
| HSET | 291K | 278K | **1.05x** |
| SPOP | 289K | 301K | 0.96x |
| ZADD | 295K | 301K | 0.98x |
| ZPOPMIN | 292K | 301K | 0.97x |

At pipeline=1, both servers are network-bound. Performance is roughly equal across all commands.

## How to reproduce

```bash
./bench.sh
```

Requires `redis-server` and `redis-benchmark` in PATH.
