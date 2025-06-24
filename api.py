# api.py
from typing import Dict, List
from .partitioning.sharder import Sharder
from .replication.replication_manager import ReplicationManager
from .storage_engine.encoding_pb2 import ValueData # type: ignore
from .storage_engine.cold_storage import COLD_STORAGE_DIR as BASE_COLD_STORAGE_ROOT_DIR # Import path cold storage

class KeyValueStore:
    def __init__(self, num_shards: int = 2):
        self.num_shards = num_shards
        self.sharder = Sharder(num_shards)
        # Setiap shard akan memiliki instance ReplicaManager-nya sendiri
        self.shards: Dict[int, ReplicationManager] = {
            i: ReplicationManager(
                shard_id=i,
                total_shards=num_shards,
                base_cold_dir=BASE_COLD_STORAGE_ROOT_DIR # Teruskan path cold storage yang sudah dihitung
            )
            for i in range(num_shards)
        }

    def put(self, key: str, value: str): # value sekarang bertipe string
        shard_id = self.sharder.get_shard_id(key)
        value_data_pb2 = ValueData(data=value) # Inisialisasi ValueData dengan string
        self.shards[shard_id].put_data(key, value_data_pb2)
        # Logging/print dari PUT dipindahkan ke main.py atau replication_manager.py

    def get(self, key: str) -> str | None: # return type sekarang string atau None
        shard_id = self.sharder.get_shard_id(key)
        value_data_pb2 = self.shards[shard_id].get_data(key)
        if value_data_pb2:
            return value_data_pb2.data # Mengembalikan string
        return None

    def shutdown(self):
        # Memastikan semua data di hot storage di-flush ke cold storage saat shutdown
        for shard_id in self.shards:
            self.shards[shard_id].flush_hot_to_cold()
        # logging.info("Key-Value Store shut down. All hot data flushed.") # Logging dari main.py