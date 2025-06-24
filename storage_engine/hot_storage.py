# storage_engine/hot_storage.py
import logging

class HotStorage:
    def __init__(self):
        self.store = {} # Ini adalah hashmap (dictionary Python) di memori RAM

    def put(self, key: str, value_data_pb2):
        self.store[key] = value_data_pb2
        logging.debug(f"[HotStorage DEBUG] PUT: Key='{key}' to in-memory hashmap. Total items: {len(self.store)}")

    def get(self, key: str):
        data = self.store.get(key)
        if data:
            logging.debug(f"[HotStorage DEBUG] GET: Key='{key}' FOUND in in-memory hashmap.")
        else:
            logging.debug(f"[HotStorage DEBUG] GET: Key='{key}' NOT FOUND in in-memory hashmap.")
        return data

    def remove(self, key: str):
        if key in self.store:
            del self.store[key]
            logging.debug(f"[HotStorage DEBUG] REMOVE: Key='{key}' removed from in-memory hashmap. Total items: {len(self.store)}")
            return True
        return False

    def get_all_keys(self):
        return list(self.store.keys())

    def print_hot_store(self):
        """Mencetak isi hashmap hot storage."""
        header = "--- Hot Storage Content ---"
        print(header)
        if not self.store:
            print("    (Empty)")
            print("-------------------------")
            return

        for key, value_pb in self.store.items():
            # value_pb.data sekarang adalah string, jadi cetak dengan kutip
            print(f"    '{key}': '{value_pb.data}'")
        print("-------------------------")