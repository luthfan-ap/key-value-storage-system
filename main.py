# main.py
import sys
import time
import os
import logging

# Konfigurasi logging:
# - level=logging.INFO: Hanya menampilkan pesan INFO, WARNING, ERROR, CRITICAL. (Default saat run)
# - level=logging.DEBUG: Menampilkan semua pesan, termasuk DEBUG dari ColdStorage/HotStorage. (Untuk debugging detail)
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

from .api import KeyValueStore

# Fungsi untuk menjalankan mode interaktif
def run_interactive_mode(store: KeyValueStore):
    print("== Key-Value Storage System ==")
    print("Commands:")
    print("  PUT <key> <value>      : Input data (e.g., PUT name Luthfan)")
    print("  GET <key>              : Retrieve data (e.g., GET name)")
    print("  .showall               : Display current hot storage content across all shards")
    print("  .quit                  : Shutdown the storage system and exit")
    print("\n--- Performance Benchmarks (Run via CLI arguments) ---")
    print("  BENCHMARK_PUT <num>    : Run PUT throughput/latency test (e.g., BENCHMARK_PUT 1000)")
    print("  BENCHMARK_GET <num>    : Run GET throughput/latency test (e.g., BENCHMARK_GET 1000)")
    print("  BENCHMARK_HOT_GET <num>: Run hot GET throughput/latency test (e.g., BENCHMARK_HOT_GET 1000)")
    print("  FAULT_TEST             : Run fault tolerance simulation")
    print("  PUT_FOR_COLD_BENCHMARK <num> : Prepare data for cold GET test (then restart and run GET_COLD_BENCHMARK)")
    print("  GET_COLD_BENCHMARK <num> : Run GET from Cold Storage test")
    print("\n")

    while True:
        user_input = input("> ").strip()
        parts = user_input.split(maxsplit=2)

        if not parts:
            continue

        statement = parts[0].lower()

        if statement == "put":
            if len(parts) >= 3:
                key = parts[1]
                value = " ".join(parts[2:]) # Value diambil sebagai string
                store.put(key, value) # Panggil put dengan string value
                logging.info(f"PUT command executed for key: '{key}' with value: '{value}'")
            else:
                print("Usage: PUT <key> <value>")
        elif statement == "get":
            if len(parts) == 2:
                key = parts[1]
                result = store.get(key)
                print(f"Result for '{key}': '{result}'") # Cetak dengan kutip karena string
            else:
                print("Usage: GET <key>")
        elif statement == ".showall":
            print("\n--- Displaying Hot Storage Content Across All Shards ---")
            if not store.shards:
                print("No shards initialized.")
            else:
                for shard_id, replica_manager_instance in store.shards.items():
                    # print_hot_store() sekarang tidak menerima shard_id lagi
                    replica_manager_instance.hot_storage.print_hot_store()
            print("--- End of Hot Storage Display ---")
        elif statement == ".quit":
            print("\n--- Shutting down store (flushing hot data) ---")
            store.shutdown()
            break
        else:
            print(f"Error: Unknown command '{statement}'. Please refer to usage.")
        print("-" * 30)

# ====================================================================================
# BAGIAN UTAMA UNTUK EKSEKUSI DARI CLI (if __name__ == "__main__":)
# ====================================================================================

if __name__ == "__main__":
    # Anda bisa mengatur jumlah shard di sini, misalnya 2 atau 4
    store = KeyValueStore(num_shards=2)

    if len(sys.argv) > 1: # Jika ada argumen CLI yang diberikan
        command = sys.argv[1].upper()
        num_operations = int(sys.argv[2]) if len(sys.argv) > 2 and sys.argv[2].isdigit() else 1000

        # --- 1 & 2. THROUGHPUT & LATENCY ---
        if command == "BENCHMARK_PUT":
            latencies = []
            print(f"--- Benchmarking PUT operations ({num_operations} ops) ---")
            start_total_time = time.perf_counter()
            for i in range(num_operations):
                key = f"key_{i}"
                value = f"value_string_{i}" # Value berupa string
                op_start_time = time.perf_counter()
                store.put(key, value) # Panggil store.put dengan string value
                op_end_time = time.perf_counter()
                latencies.append(op_end_time - op_start_time)
            end_total_time = time.perf_counter()

            total_time = end_total_time - start_total_time
            avg_latency = sum(latencies) / num_operations
            min_latency = min(latencies)
            max_latency = max(latencies)

            print(f"Total time for {num_operations} PUTs: {total_time:.4f} seconds")
            print(f"PUT Throughput: {num_operations / total_time:.2f} ops/second")
            print(f"PUT Latency (Avg): {avg_latency * 1000:.4f} ms")
            print(f"PUT Latency (Min): {min_latency * 1000:.4f} ms")
            print(f"PUT Latency (Max): {max_latency * 1000:.4f} ms")

        elif command == "BENCHMARK_GET":
            # Asumsi data sudah di-PUT (misalnya dari BENCHMARK_PUT atau PUT_FOR_COLD_BENCHMARK sebelumnya)
            get_latencies = []
            print(f"--- Benchmarking GET operations ({num_operations} ops) ---")
            start_total_time = time.perf_counter()
            for i in range(num_operations):
                key = f"key_{i}" # Asumsi key yang sama dengan PUT benchmark
                op_start_time = time.perf_counter()
                store.get(key)
                op_end_time = time.perf_counter()
                get_latencies.append(op_end_time - op_start_time)
            end_total_time = time.perf_counter()

            total_time = end_total_time - start_total_time
            avg_latency = sum(get_latencies) / num_operations
            min_latency = min(get_latencies)
            max_latency = max(get_latencies)

            print(f"Total time for {num_operations} GETs: {total_time:.4f} seconds")
            print(f"GET Throughput: {num_operations / total_time:.2f} ops/second")
            print(f"GET Latency (Avg): {avg_latency * 1000:.4f} ms")
            print(f"GET Latency (Min): {min_latency * 1000:.4f} ms")
            print(f"GET Latency (Max): {max_latency * 1000:.4f} ms")
        
        elif command == "BENCHMARK_HOT_GET":
            put_latencies_hot_get = []
            get_latencies_hot_get = []
            print(f"--- Benchmarking GET from Hot Storage ({num_operations} ops) ---")
            print("1. Performing PUT operations (to Hot Storage)...")
            # Lakukan PUT dulu untuk mengisi Hot Storage
            for i in range(num_operations):
                key = f"hot_test_key_{i}"
                value = f"hot_test_value_{i}"
                op_start_time = time.perf_counter()
                store.put(key, value)
                op_end_time = time.perf_counter()
                put_latencies_hot_get.append(op_end_time - op_start_time)
            avg_put_latency_hot_get = sum(put_latencies_hot_get) / num_operations
            print(f"   (PUT Avg Latency: {avg_put_latency_hot_get * 1000:.4f} ms)")

            print("\n2. Performing GET operations (should hit Hot Storage)...")
            start_get_hot = time.perf_counter()
            for i in range(num_operations):
                key = f"hot_test_key_{i}"
                op_start_time = time.perf_counter()
                store.get(key) # Ini akan mengenai Hot Storage
                op_end_time = time.perf_counter()
                get_latencies_hot_get.append(op_end_time - op_start_time)
            end_get_hot = time.perf_counter()
            total_time_get_hot = end_get_hot - start_get_hot
            avg_get_hot_latency = sum(get_latencies_hot_get) / num_operations

            print(f"Total time for {num_operations} GETs (Hot Storage): {total_time_get_hot:.4f} seconds")
            print(f"GET Latency (Avg - Hot Storage): {avg_get_hot_latency * 1000:.4f} ms")
            print(f"GET Throughput (Hot Storage): {num_operations / total_time_get_hot:.2f} ops/second")

        # --- 3. FAULT TOLERANCE ---
        elif command == "FAULT_TEST":
            key_to_test = "faulty_key_001"
            value_to_test = "data_for_fault_test_string" # Value berupa string
            shard_id = store.sharder.get_shard_id(key_to_test)
            target_rm = store.shards[shard_id]

            print(f"\n--- Fault Tolerance Test for key '{key_to_test}' on Shard {shard_id} ---")

            print("1. PUT data to system (goes to Hot Storage).")
            store.put(key_to_test, value_to_test)
            print("   (cold_data directories should be empty for this key initially)")

            print("\n2. FLUSH data to Cold Storage (replicated).")
            target_rm.flush_hot_to_cold()
            print("   Verifying presence in both cold replicas (expected).")

            print("\n3. Simulating failure: Deleting data from PRIMARY cold replica (replica_0).")
            primary_filepath = os.path.join(target_rm.cold_storage.storage_dir, f"{key_to_test}.bin")
            if os.path.exists(primary_filepath):
                os.remove(primary_filepath)
                print(f"   Deleted: {primary_filepath}")
            else:
                print(f"   Primary replica file not found for deletion: {primary_filepath}")

            print("\n4. Attempting to GET data (should now come from replica_1).")
            result = store.get(key_to_test)
            print(f"   GET result: '{result}' (expected: '{value_to_test}')")
            if result == value_to_test:
                print("   SUCCESS: Data retrieved despite primary replica failure.")
            else:
                print("   FAILURE: Data not retrieved correctly after primary replica failure.")

            print("\n5. Simulating failure: Deleting data from SECONDARY cold replica (replica_1).")
            secondary_filepath = os.path.join(target_rm.replica_cold_storage.storage_dir, f"{key_to_test}.bin")
            if os.path.exists(secondary_filepath):
                os.remove(secondary_filepath)
                print(f"   Deleted: {secondary_filepath}")
            else:
                print(f"   Secondary replica file not found for deletion: {secondary_filepath}")

            print("\n6. Attempting to GET data again (should now fail).")
            result = store.get(key_to_test)
            print(f"   GET result: '{result}' (expected: None)")
            if result is None:
                print("   SUCCESS: Data confirmed lost when both replicas are gone.")
            else:
                print("   FAILURE: Data still retrieved when both replicas should be gone.")

            print("\n--- Fault Tolerance Test Complete ---")

        # --- 4. HOT vs COLD COMPARISON ---
        elif command == "PUT_FOR_COLD_BENCHMARK":
            num_ops = int(sys.argv[2]) if len(sys.argv) > 2 and sys.argv[2].isdigit() else 100
            for i in range(num_ops):
                store.put(f"cold_test_key_{i}", f"cold_test_value_{i}") # Value berupa string
            print(f"Finished putting {num_ops} items for cold storage benchmark. Data will be flushed on shutdown.")

        elif command == "GET_COLD_BENCHMARK":
            num_ops = int(sys.argv[2]) if len(sys.argv) > 2 and sys.argv[2].isdigit() else 100
            get_cold_latencies = []
            print(f"--- Measuring GET from Cold Storage ({num_ops} ops) ---")
            print("  (Assuming hot storage is empty, reading from disk first)")
            start_time_cold = time.perf_counter()
            for i in range(num_ops):
                key = f"cold_test_key_{i}"
                op_start_time = time.perf_counter()
                store.get(key)
                op_end_time = time.perf_counter()
                get_cold_latencies.append(op_end_time - op_start_time)
            end_time_cold = time.perf_counter()
            total_time_cold = end_time_cold - start_time_cold
            avg_cold_latency = sum(get_cold_latencies) / num_ops

            print(f"Total time for {num_ops} GETs (Cold Storage): {total_time_cold:.4f} seconds")
            print(f"GET Latency (Avg - Cold Storage): {avg_cold_latency * 1000:.4f} ms")

        # --- Perintah CLI tunggal (PUT, GET) ---
        elif command == "PUT":
            if len(sys.argv) >= 4:
                key = sys.argv[2]
                value = " ".join(sys.argv[3:]) # Value berupa string
                store.put(key, value)
                logging.info(f"CLI PUT command executed for key: '{key}' with value: '{value}'")
            else:
                print("Usage: python -m key_value_storage_system.main PUT <key> <value>")
        elif command == "GET":
            if len(sys.argv) == 3:
                key = sys.argv[2]
                result = store.get(key)
                print(f"Result for '{key}': '{result}'") # Cetak dengan kutip karena string
            else:
                print("Usage: python -m key_value_storage_system.main GET <key>")
        
        else:
            print(f"Error: Unknown command '{command}'.")

    else: # Jika tidak ada argumen CLI, masuk mode interaktif
        run_interactive_mode(store)

    # Bagian shutdown ini akan selalu dijalaiakan di akhir eksekusi program
    logging.info("\n--- Shutting down store (flushing hot data) ---")
    store.shutdown()