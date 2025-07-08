# Key-Value Storage System

A distributed key-value storage system with support for sharding, replication, and a two-tier storage architecture (hot/cold storage).

## Features

- **Two-Tier Storage Architecture**
  - Hot Storage (In-Memory): Fast access using Python dictionaries
  - Cold Storage (Disk-Based): Persistent storage using Protocol Buffers serialization

- **Data Distribution**
  - Sharding: Distributes data across multiple shards using key-based hashing
  - Replication: Each shard maintains primary and secondary replicas for fault tolerance

- **Fault Tolerance**
  - Replica Management: Automatic failover between primary and secondary cold storage
  - Data Consistency: Synchronous replication for cold storage writes

- **Performance Optimizations**
  - Hot Cache: Frequently accessed data stored in memory
  - Automatic Promotion: Cold data promoted to hot storage on access

## Architecture

```
┌─────────────────┐
│  KeyValueStore  │
└────────┬────────┘
         │
    ┌────┴────┐
    │ Sharder │
    └────┬────┘
         │
    ┌────┴───────────────┐
    │ ReplicationManager │
    └────┬────────┬──────┘
         │        │
   ┌─────┴───┐ ┌─┴────────┐
   │   Hot   │ │   Cold   │
   │ Storage │ │ Storage  │
   └─────────┘ └──────────┘
```

## Installation

1. Clone the repository
2. Ensure you have Python 3.12+ installed
3. Install Protocol Buffers compiler (protoc)

## Usage

### Interactive Mode

```bash
python -m main.py
```

Commands:
- `PUT <key> <value>` - Store a key-value pair
- `GET <key>` - Retrieve a value by key
- `.showall` - Display current hot storage content
- `.quit` - Shutdown the system

### CLI Mode

```bash
# Put a value
python -m main.py PUT key1 "value1"

# Get a value
python -m main.py GET key1
```

### Benchmarking

```bash
# Run PUT benchmark (1000 operations)
python -m main.py BENCHMARK_PUT 1000

# Run GET benchmark (1000 operations)
python -m main.py BENCHMARK_GET 1000

# Run hot storage GET benchmark
python -m main.py BENCHMARK_HOT_GET 1000

# Run cold storage benchmarks
python -m main.py PUT_FOR_COLD_BENCHMARK 1000
python -m main.py GET_COLD_BENCHMARK 1000
```

### Fault Tolerance Testing

```bash
python -m main.py FAULT_TEST
```

## Project Structure

```bash
key-value-storage-system/
├── api.py                # Main API interface
├── main.py               # CLI and interactive interface
├── partitioning/         # Sharding logic
│   └── sharder.py
├── replication/          # Replication management
│   └── replication_manager.py
├── storage_engine/       # Storage implementations
│   ├── hot_storage.py    # In-memory storage
│   ├── cold_storage.py   # Disk-based storage
│   ├── encoding.proto    # Protocol Buffers schema
│   └── encoding_pb2.py   # Generated Protocol Buffers code
└── cold_data/            # Persistent storage directory
    ├── shard_0_replica_0/
    ├── shard_0_replica_1/
    ├── shard_1_replica_0/
    └── shard_1_replica_1/
```

## General Workflows

- Data is first written to hot storage (in-memory)
- Data is flushed to cold storage (disk) on:
  - Explicit flush operations
  - System shutdown
- Cold storage uses Protocol Buffers for efficient serialization
- Each shard maintains two replicas for fault tolerance
- Automatic failover between replicas if primary storage fails

## Detailed Workflows

### Startup
- `main.py` creates a `KeyValueStore` instance with 2 shards.
- Each shard is managed by a `ReplicationManager`, which has:
    - Its own `HotStorage` (in-memory)
    - Two `ColdStorage` replicas (on disk)

### PUT Workflow
1. User runs (via CLI):
    ```
    PUT <key> <value>
    ```
2. `main.py` calls `KeyValueStore.put(key,value)`
3. `KeyValueStore` uses `Sharder` to pick the shard for the key.
4. The corresponding `ReplicationManager` receives the PUT.
5. The value is wrapped in a `ValueData` protobuf message.
6. The value is stored in the shard's `HotStorage` (RAM only).
    - `ColdStorage` is not updated yet (unless a flush or shutdown happens).

### GET Workflow
1. User runs:
    ```
    GET <key>
    ```
2. `main.py` calls `KeyValueStore.get(key)`
3. `KeyValueStore` uses `Sharder` to pick the shard.
4. The corresponding `ReplicationManager` receives the GET.
5. The manager tries to get the value:
    - First from __HotStorage__ (RAM)
    - If not found, then from __primary ColdStorage__ (disk)
    - If not found, from __replica ColdStorage__ (disk)
6. If found in cold storage, the value is promoted to hot storage for faster future access.


## Limitations

- No automatic rebalancing of shards
- Synchronous replication may impact write performance
- Database evolution hasn't been fully implemented (still a work in progress)

## Future Improvements

- Implement automatic shard rebalancing
- Add data compression for cold storage
- Implement asynchronous replication option
- Add support for range queries
- Implement backup and restore functionality
