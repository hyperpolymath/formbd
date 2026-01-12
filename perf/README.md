# FormDB Performance Module

Performance optimization features for FormDB including caching, connection pooling, batch operations, and metrics.

## Features

| Feature | File | Description |
|---------|------|-------------|
| Query Cache | `FormDB_Perf_Cache.res` | LRU cache for compiled query plans |
| Connection Pool | `FormDB_Perf_Pool.res` | Connection pooling for backends |
| Batch Operations | `FormDB_Perf_Batch.res` | Batch processing for throughput |
| Metrics | `FormDB_Perf_Metrics.res` | Performance monitoring |

## Query Plan Cache

LRU cache for compiled query plans with TTL-based expiration.

```rescript
// Cache a query plan
cachePlan("SELECT * FROM users", compiledPlan)

// Get cached plan
switch getCachedPlan("SELECT * FROM users") {
| Some(plan) => // Use cached plan
| None => // Compile new plan
}

// Get cache stats
let {size, maxSize, hitRate} = stats(queryPlanCache)
```

### Configuration

```rescript
let cache = make(
  ~maxSize=1000,      // Max entries
  ~ttlMs=300000.0     // 5 minute TTL
)
```

## Connection Pool

Connection pooling with automatic scaling and idle cleanup.

```rescript
// Create pool
let pool = make(~config={
  minConnections: 2,
  maxConnections: 10,
  idleTimeoutMs: 30000.0,
  acquireTimeoutMs: 5000.0,
})

// Acquire connection
let conn = await acquire(pool)

// Use connection...

// Release connection
release(pool, conn)

// Get pool stats
let {total, idle, inUse, waiting} = stats(pool)
```

## Batch Operations

Batch processing for improved write throughput.

```rescript
// Batch insert
batchInsert("users", [doc1, doc2, doc3])

// Batch update
batchUpdate("users", [
  {id: "1", document: update1},
  {id: "2", document: update2},
])

// Batch delete
batchDelete("users", ["1", "2", "3"])

// Flush batch
let result = await flush(globalProcessor)
// result: {successful: 10, failed: 0, errors: []}
```

### Configuration

```rescript
let processor = make(~config={
  maxBatchSize: 100,
  flushIntervalMs: 100.0,
  retryOnFailure: true,
  maxRetries: 3,
})
```

## Metrics

Real-time performance metrics with Prometheus export.

```rescript
// Record query
recordQuery(latencyMs)

// Record cache hit/miss
recordCacheHit()
recordCacheMiss()

// Record error
recordError()

// Export Prometheus format
let metrics = exportPrometheus()
// formdb_query_total 1234
// formdb_query_latency_ms 5.2
// formdb_cache_hits_total 890
// ...
```

### Pre-defined Metrics

| Metric | Type | Description |
|--------|------|-------------|
| `formdb_query_total` | Counter | Total queries executed |
| `formdb_query_latency_ms` | Gauge | Last query latency |
| `formdb_cache_hits_total` | Counter | Cache hits |
| `formdb_cache_misses_total` | Counter | Cache misses |
| `formdb_connection_pool_size` | Gauge | Pool size |
| `formdb_active_connections` | Gauge | In-use connections |
| `formdb_batch_size` | Gauge | Current batch size |
| `formdb_errors_total` | Counter | Total errors |

## Architecture

```
perf/
├── README.md
└── src/
    ├── FormDB_Perf_Cache.res    # Query plan cache
    ├── FormDB_Perf_Pool.res     # Connection pooling
    ├── FormDB_Perf_Batch.res    # Batch operations
    └── FormDB_Perf_Metrics.res  # Performance metrics
```

## Best Practices

### Caching
- Enable query caching for read-heavy workloads
- Tune TTL based on data freshness requirements
- Monitor hit rate and adjust cache size

### Connection Pooling
- Set min connections based on baseline load
- Set max connections based on backend capacity
- Enable idle cleanup for variable workloads

### Batching
- Use batch operations for bulk imports
- Tune batch size for optimal throughput
- Enable retry for transient failures

### Monitoring
- Export metrics to Prometheus/Grafana
- Set up alerts for error rates
- Track latency percentiles

## License

AGPL-3.0-or-later
