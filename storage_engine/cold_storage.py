import os
from .encoding_pb2 import KeyValue, ValueData

COLD_STORAGE_DIR = "cold_data"

class ColdStorage:
    def __init__(self, storage_dir: str = COLD_STORAGE_DIR):
        self.storage_dir = storage_dir
        os.makedirs(self.storage_dir, exist_ok=True)

    def _get_filepath(self, key: str) -> str:
        return os.path.join(self.storage_dir, f"{key}.bin")

    def put(self, key: str, value_data_pb2: ValueData):
        filepath = self._get_filepath(key)
        kv_message = KeyValue(key=key, value=value_data_pb2)
        with open(filepath, "wb") as f:
            f.write(kv_message.SerializeToString())

    def get(self, key: str) -> ValueData | None:
        filepath = self._get_filepath(key)
        if not os.path.exists(filepath):
            return None
        try:
            with open(filepath, "rb") as f:
                kv_message = KeyValue()
                kv_message.ParseFromString(f.read())
                return kv_message.value
        except Exception as e:
            print(f"Error reading cold storage for {key}: {e}")
            return None

    def delete(self, key: str):
        filepath = self._get_filepath(key)
        if os.path.exists(filepath):
            os.remove(filepath)
            return True
        return False