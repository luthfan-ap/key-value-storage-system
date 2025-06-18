from typing import Dict, List
from .partitioning.sharder import Sharder
from .replication.replica_manager import ReplicaManager
from .storage_engine.encoding_pb2 import ValueData

class KeyValueStore:
    def __init__(self, num_shards: int = 4): # Example: 4 shards
        self.num_shards = num_shards
        self.sharder = Sharder(num_shards)
        # Each shard will have its own ReplicaManager instance
        self.shards: Dict[int, ReplicaManager] = {
            i: ReplicaManager(shard_id=i, total_shards=num_shards) for i in range(num_shards)
        }

    def put(self, key: str, value: str):
        shard_id = self.sharder.get_shard_id(key)
        value_data_pb2 = ValueData(data=value) # Create protobuf message
        self.shards[shard_id].put_data(key, value_data_pb2)
        print(f"PUT: Key '{key}' mapped to shard {shard_id}")

    def get(self, key: str) -> str | None:
        shard_id = self.sharder.get_shard_id(key)
        value_data_pb2 = self.shards[shard_id].get_data(key)
        if value_data_pb2:
            print(f"GET: Key '{key}' found on shard {shard_id}")
            return value_data_pb2.data
        print(f"GET: Key '{key}' not found on shard {shard_id}")
        return None

    def shutdown(self):
        # Optionally, flush all hot data to cold storage on shutdown
        for shard_id in self.shards:
            self.shards[shard_id].flush_hot_to_cold()
        print("Key-Value Store shut down. All hot data flushed.")