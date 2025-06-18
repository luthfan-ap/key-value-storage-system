# main.py
from api import KeyValueStore
import time

if __name__ == "__main__":
    store = KeyValueStore(num_shards=2) # Start with 2 shards for testing

    print("--- PUT Operations ---")
    store.put("name", "Lutan")
    store.put("city", "Surabaya")
    store.put("age", "30")
    store.put("occupation", "Engineer")
    store.put("hobby", "Reading")
    store.put("country", "Indonesia")

    print("\n--- GET Operations ---")
    print(f"Get 'name': {store.get('name')}")
    print(f"Get 'city': {store.get('city')}")
    print(f"Get 'non_existent_key': {store.get('non_existent_key')}")
    print(f"Get 'age': {store.get('age')}")

    # Demonstrate schema evolution by adding a new field (conceptually)
    # First, modify data_encoding.proto:
    # message ValueData {
    #     string data = 1;
    #     int32 timestamp = 2; // New field
    # }
    # Then re-run: protoc --python_out=./storage_engine storage_engine/data_encoding.proto
    # And update how you create ValueData:
    # value_data_pb2 = ValueData(data=value, timestamp=int(time.time()))

    print("\n--- Testing Data Hot/Cold Movement (Conceptual) ---")
    # In a real scenario, you'd have a background thread or a more complex policy
    # to decide when to move data to cold storage.
    # For this example, let's manually trigger a move for one key.
    shard_id_for_hobby = store.sharder.get_shard_id("hobby")
    store.shards[shard_id_for_hobby].move_to_cold("hobby")
    print(f"Get 'hobby' after potential move: {store.get('hobby')}") # Should still be retrievable from cold

    print("\n--- Shutting down store (flushing hot data) ---")
    store.shutdown()