from typing import List, Dict
from ..storage_engine.hot_storage import HotStorage
from ..storage_engine.cold_storage import ColdStorage
from ..storage_engine.encoding_pb2 import ValueData #type:ignore

class ReplicationManager:
    def __init__(self, shard_id: int, total_shards: int, base_cold_dir: str = "cold_data"):
        self.shard_id = shard_id
        self.total_shards = total_shards
        self.hot_storage = HotStorage()
        # Each replica will have its own cold storage directory
        self.cold_storage = ColdStorage(f"{base_cold_dir}/shard_{shard_id}_replica_0")
        self.replica_cold_storage = ColdStorage(f"{base_cold_dir}/shard_{shard_id}_replica_1")

    def put_data(self, key: str, value_data_pb2: ValueData):
        # Write to primary hot storage
        self.hot_storage.put(key, value_data_pb2)
        
        # Write to primary cold storage
        # self.cold_storage.put(key, value_data_pb2)

        # Synchronous replication to the second replica
        # self.replica_cold_storage.put(key, value_data_pb2)
        # print(f"Shard {self.shard_id}: Key '{key}' replicated to primary and secondary cold storage.")

    def get_data(self, key: str):
        # Try hot storage first
        data = self.hot_storage.get(key)
        if data:
            return data

        # If not in hot storage, try primary cold storage
        data = self.cold_storage.get(key)
        if data:
            # Optionally, promote to hot storage on read
            self.hot_storage.put(key, data)
            return data

        # If still not found, try replica cold storage (for robustness, though primary should have it)
        data = self.replica_cold_storage.get(key)
        if data:
            self.hot_storage.put(key, data) # Promote
            return data
        return None

    # Add methods for handling data movement between hot/cold based on access patterns
    # For this project, you can start simple: data written to hot and cold,
    # and on read from cold, it's moved to hot.
    def move_to_cold(self, key: str):
        # Example: move data from hot to cold if not accessed for a while
        value = self.hot_storage.get(key)
        if value:
            self.hot_storage.remove(key)
            self.cold_storage.put(key, value)
            self.replica_cold_storage.put(key, value) # Replicate the move
            print(f"Key '{key}' moved from hot to cold storage on shard {self.shard_id}.")

    def flush_hot_to_cold(self):
        # Periodically flush all hot data to cold storage for persistence
        for key in self.hot_storage.get_all_keys():
            value = self.hot_storage.get(key)
            if value:
                self.cold_storage.put(key, value)
                self.replica_cold_storage.put(key, value)
                self.hot_storage.remove(key)
        print(f"Shard {self.shard_id}: All hot data flushed to cold storage.")