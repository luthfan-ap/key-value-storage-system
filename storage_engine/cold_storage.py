# storage_engine/cold_storage.py
import os
import logging
from .encoding_pb2 import KeyValue, ValueData # type: ignore

# --- PENTING: Perhitungan COLD_STORAGE_DIR untuk memastikan lokasi yang benar ---
_CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.normpath(os.path.join(_CURRENT_DIR, "..")) # Navigasi ke direktori root proyek
COLD_STORAGE_DIR = os.path.join(PROJECT_ROOT, "cold_data") # cold_data di dalam root proyek
# ---

class ColdStorage:
    def __init__(self, storage_dir: str = COLD_STORAGE_DIR):
        self.storage_dir = storage_dir
        try:
            os.makedirs(self.storage_dir, exist_ok=True)
            logging.debug(f"[ColdStorage] Directory {self.storage_dir} ensured to exist.")
        except OSError as e:
            logging.error(f"[ColdStorage ERROR] Could not create directory {self.storage_dir}: {e}")

    def _get_filepath(self, key: str) -> str:
        """Mendapatkan path file biner untuk kunci tertentu."""
        return os.path.join(self.storage_dir, f"{key}.bin")

    def put(self, key: str, value_data_pb2: ValueData):
        """
        Menyimpan data ke cold storage (disk).
        Data ValueData dienkode menjadi biner Protocol Buffers.
        """
        filepath = self._get_filepath(key)
        kv_message = KeyValue(key=key, value=value_data_pb2)
        try:
            serialized_data = kv_message.SerializeToString()
            with open(filepath, "wb") as f: # Buka file dalam mode tulis biner ('wb')
                f.write(serialized_data) # Tulis data biner ke file
            logging.debug(f"[ColdStorage] Successfully wrote data for key '{key}' to {filepath}")
        except Exception as e:
            logging.error(f"[ColdStorage ERROR] Failed to write data for key '{key}' to {filepath}: {e}")

    def get(self, key: str) -> ValueData | None:
        """
        Mengambil data dari cold storage (disk) dan mendeserialisasinya.
        """
        filepath = self._get_filepath(key)
        if not os.path.exists(filepath):
            return None
        try:
            with open(filepath, "rb") as f: # Buka file dalam mode baca biner ('rb')
                raw_data = f.read()
                kv_message = KeyValue()
                kv_message.ParseFromString(raw_data)
                logging.debug(f"[ColdStorage] Read data for key '{key}' from {filepath}")
                return kv_message.value
        except Exception as e:
            logging.error(f"Error reading cold storage for {key}: {e}")
            return None

    def delete(self, key: str):
        """Menghapus data dari cold storage."""
        filepath = self._get_filepath(key)
        if os.path.exists(filepath):
            os.remove(filepath)
            logging.debug(f"[ColdStorage] Deleted file for key '{key}': {filepath}")
            return True
        return False