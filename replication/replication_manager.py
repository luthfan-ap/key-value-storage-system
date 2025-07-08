# replication/replication_manager.py
import os
import logging
from typing import List, Dict
from ..storage_engine.hot_storage import HotStorage
from ..storage_engine.cold_storage import ColdStorage
from ..storage_engine.encoding_pb2 import ValueData # type: ignore

class ReplicationManager:
    def __init__(self, shard_id: int, total_shards: int, base_cold_dir: str):
        self.shard_id = shard_id
        self.total_shards = total_shards
        self.hot_storage = HotStorage()

        # Inisialisasi cold storage dan replica cold storage
        # Menggunakan os.path.join untuk membangun path direktori untuk setiap replika
        self.cold_storage = ColdStorage(os.path.join(base_cold_dir, f"shard_{shard_id}_replica_0"))
        self.replica_cold_storage = ColdStorage(os.path.join(base_cold_dir, f"shard_{shard_id}_replica_1"))

        logging.info(f"Shard {self.shard_id}: Initialized. Primary Cold at {self.cold_storage.storage_dir}. Replica Cold at {self.replica_cold_storage.storage_dir}")

    def put_data(self, key: str, value_data_pb2: ValueData):
        # Data PUT hanya masuk ke Hot Storage (memori) terlebih dahulu
        self.hot_storage.put(key, value_data_pb2)
        logging.info(f"Shard {self.shard_id}: Key '{key}' PUT into Hot Storage only.")

    def get_data(self, key: str):
        # LANGKAH 1: Coba baca dari hot storage (hashmap di memori)
        data = self.hot_storage.get(key)
        if data:
            logging.info(f"[GET Flow] Shard {self.shard_id}: Key '{key}' FOUND in Hot Storage.")
            return data

        # LANGKAH 2: Jika TIDAK ditemukan di hot storage, coba cold storage utama
        logging.info(f"[GET Flow] Shard {self.shard_id}: Key '{key}' NOT FOUND in Hot. Checking Primary Cold ({self.cold_storage.storage_dir})...")
        data = self.cold_storage.get(key)
        if data:
            logging.info(f"[GET Flow] Shard {self.shard_id}: Key '{key}' FOUND in Primary Cold. Promoting to Hot.")
            self.hot_storage.put(key, data) # Promote ke hot storage
            return data

        # LANGKAH 3: Jika masih TIDAK ditemukan, coba replika cold storage sekunder
        logging.info(f"[GET Flow] Shard {self.shard_id}: Key '{key}' NOT FOUND in Primary Cold. Checking Replica Cold ({self.replica_cold_storage.storage_dir})...")
        data = self.replica_cold_storage.get(key)
        if data:
            logging.info(f"[GET Flow] Shard {self.shard_id}: Key '{key}' FOUND in Replica Cold. Promoting to Hot.")
            self.hot_storage.put(key, data) # Promote
            return data

        logging.warning(f"[GET Flow] Shard {self.shard_id}: Key '{key}' NOT FOUND anywhere.")
        return None

    def move_to_cold(self, key: str):
        # Operasi pemindahan eksplisit dari hot ke cold
        value = self.hot_storage.get(key)
        if value:
            self.hot_storage.remove(key) # Hapus dari hot
            self.cold_storage.put(key, value) # Tulis ke cold
            self.replica_cold_storage.put(key, value) # Replikasi ke cold
            logging.info(f"Shard {self.shard_id}: Key '{key}' explicitly MOVED from hot to cold storage.")
        else:
            logging.warning(f"Shard {self.shard_id}: Attempted to move non-existent key '{key}' from hot to cold.")

    def flush_hot_to_cold(self):
        # Proses utama untuk memindahkan semua data dari hot ke cold storage
        logging.info(f"Shard {self.shard_id}: Starting flush of Hot Storage to Cold Storage.")
        keys_to_flush = self.hot_storage.get_all_keys()
        if not keys_to_flush:
            logging.info(f"Shard {self.shard_id}: Hot Storage is empty, nothing to flush.")
            return

        for key in list(keys_to_flush): # Gunakan list() karena dictionary akan dimodifikasi saat iterasi
            value = self.hot_storage.get(key)
            if value:
                self.cold_storage.put(key, value) # Tulis ke primary cold
                self.replica_cold_storage.put(key, value) # Replikasi ke secondary cold
                self.hot_storage.remove(key) # Hapus dari hot setelah berhasil ditulis ke cold
                logging.debug(f"Shard {self.shard_id}: Key '{key}' flushed from hot to cold.")
            else:
                logging.warning(f"Shard {self.shard_id}: Key '{key}' disappeared from hot storage during flush.")
        logging.info(f"Shard {self.shard_id}: All hot data flushed to cold storage.")