# FormDB Distributed Mode

Distributed computing features for FormDB including cluster coordination, consensus, sharding, and replication.

## Features

| Feature | File | Description |
|---------|------|-------------|
| Cluster | `FormDB_Distributed_Cluster.res` | Node discovery and membership |
| Consensus | `FormDB_Distributed_Consensus.res` | Raft-based consensus |
| Sharding | `FormDB_Distributed_Sharding.res` | Data partitioning |
| Replication | `FormDB_Distributed_Replication.res` | Data synchronization |

## Cluster Coordination

Node discovery, membership, and cluster state management.

```rescript
// Create cluster state
let cluster = makeState(~clusterId="formdb-cluster")

// Add node to cluster
addNode(cluster, {
  id: "node-1",
  address: "192.168.1.10",
  port: 7946,
  status: Active,
  role: Follower,
  lastHeartbeat: Js.Date.now(),
  metadata: Js.Dict.empty(),
})

// Check if node is leader
if isLeader(cluster, "node-1") {
  // Handle leader responsibilities
}

// Get active nodes
let activeNodes = getActiveNodes(cluster)
```

### Node Roles

| Role | Description |
|------|-------------|
| Leader | Coordinates writes, manages cluster |
| Follower | Replicates data, handles reads |
| Candidate | Participating in leader election |

### Node Status

| Status | Description |
|--------|-------------|
| Starting | Node is initializing |
| Joining | Node is joining cluster |
| Active | Node is operational |
| Leaving | Node is gracefully leaving |
| Down | Node is unreachable |

## Consensus (Raft)

Raft-based distributed consensus for leader election and log replication.

```rescript
// Create consensus node
let node = make(~nodeId="node-1", ~electionTimeout=5000.0)

// Handle vote request from candidate
let response = handleVoteRequest(node, {
  term: 2,
  candidateId: "node-2",
  lastLogIndex: 10,
  lastLogTerm: 1,
})

// Start election if timeout elapsed
if electionTimeoutElapsed(node) {
  let request = startElection(node)
  // Send vote requests to peers
}

// Append command (leader only)
switch appendCommand(node, Js.Json.string("command")) {
| Some(entry) => // Command logged
| None => // Not leader
}
```

### Raft States

| State | Description |
|-------|-------------|
| Follower | Receives heartbeats, responds to requests |
| Candidate | Requesting votes for leader election |
| Leader | Manages log replication, sends heartbeats |

## Sharding

Consistent hashing and data partitioning.

```rescript
// Create shard manager
let manager = make(~config={
  strategy: Hash,
  numShards: 16,
  replicationFactor: 3,
  virtualNodes: 150,
})

// Initialize with nodes
initializeShards(manager, ["node-1", "node-2", "node-3"])

// Get shard for key
switch getShardForKey(manager, "user:12345") {
| Some(shard) =>
    Console.log(`Key maps to shard ${Int.toString(shard.id)}`)
    Console.log(`Primary: ${shard.primaryNode}`)
| None => ()
}

// Get node using consistent hashing
switch getNodeForKey(manager, "user:12345") {
| Some(nodeId) => // Route request to node
| None => ()
}
```

### Sharding Strategies

| Strategy | Description |
|----------|-------------|
| Hash | Consistent hashing with virtual nodes |
| Range | Key range-based partitioning |
| Directory | Lookup table-based routing |

## Replication

Data synchronization with configurable consistency levels.

```rescript
// Create replication manager
let repl = make(~nodeId="node-1", ~config={
  mode: SemiSync,
  readConsistency: One,
  writeConsistency: Quorum,
  replicationFactor: 3,
  syncIntervalMs: 100.0,
  conflictResolution: "last-write-wins",
})

// Create and queue replication event
let event = createEvent(
  repl,
  Write,
  "users",
  "user-123",
  Some(Js.Json.string("{...}")),
)
queueEvent(repl, event)

// Check if write succeeded
if isWriteSuccessful(repl, ackCount) {
  // Commit write
}

// Get replication lag
let stats = getLagStats(repl)
Console.log(`Max lag: ${Int.toString(stats.maxLag)} events`)
```

### Consistency Levels

| Level | Description | Use Case |
|-------|-------------|----------|
| One | Single node responds | Fast reads |
| Quorum | Majority responds | Balanced |
| All | All nodes respond | Strong consistency |
| LocalQuorum | Local DC quorum | Multi-region |

### Replication Modes

| Mode | Description | Latency | Durability |
|------|-------------|---------|------------|
| Synchronous | Wait for all replicas | High | High |
| Asynchronous | Fire and forget | Low | Lower |
| SemiSync | Wait for quorum | Medium | Good |

## Architecture

```
distributed/
├── README.md
└── src/
    ├── FormDB_Distributed_Cluster.res     # Cluster coordination
    ├── FormDB_Distributed_Consensus.res   # Raft consensus
    ├── FormDB_Distributed_Sharding.res    # Data sharding
    └── FormDB_Distributed_Replication.res # Data replication
```

## Deployment Patterns

### Single Region

```
┌─────────────────────────────────────────┐
│              Load Balancer              │
└────────────┬───────────┬────────────────┘
             │           │
      ┌──────▼──┐   ┌────▼────┐   ┌────────┐
      │ Node 1  │   │ Node 2  │   │ Node 3 │
      │ (Leader)│   │(Follower│   │(Follower│
      └─────────┘   └─────────┘   └─────────┘
```

### Multi-Region

```
Region A                    Region B
┌─────────────┐            ┌─────────────┐
│   Node 1    │◄──────────►│   Node 4    │
│   Node 2    │            │   Node 5    │
│   Node 3    │            │   Node 6    │
└─────────────┘            └─────────────┘
```

## Best Practices

### Cluster Sizing
- Minimum 3 nodes for fault tolerance
- Use odd number of nodes for consensus
- Scale shards based on data volume

### Consistency Tuning
- Use `One` for read-heavy workloads
- Use `Quorum` for balanced reads/writes
- Use `All` only when strong consistency required

### Replication Factor
- 3 for most workloads
- 5 for critical data
- Consider network bandwidth

## License

AGPL-3.0-or-later
