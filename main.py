from .api import KeyValueStore
import time
from .storage_engine.hot_storage import HotStorage

if __name__ == "__main__":
    store = KeyValueStore(num_shards=2)

    print("== Key-Value Storage System ==")
    print("1. Input data (Usage: PUT <key> <value>)")
    print("2. Get data (Usage: GET <key>)\n")
    # Looping for user input
    while True:
        user_input = input("> ").strip()
        parts = user_input.split(maxsplit=2)

        statement = parts[0].lower() # first argument, (PUT / GET)

        if statement == "put":
            if len(parts) >= 3: # argument template: PUT <key> <value>
                key = parts[1] # first argument, key
                
                value = " ".join(parts[2:])
                store.put(key, value)
                print(f"PUT command executed for key: '{key}'")
            else:
                print("Usage: PUT <key> <value>")
        elif statement == "get":
            if len(parts) == 2:
                key = parts[1]
                result = store.get(key)
                print(f"Result for '{key}': {result}")
            else:
                print("Usage: GET <key>")
        elif statement == ".quit": # if done, do .quit to shutdown the storage system
            print("\n--- Shutting down store (flushing hot data) ---")
            store.shutdown()
            break
        elif statement == ".showall":
            for shard_id, replica_manager_instance in store.shards.items():
                # print(f"Showing all data: \n{HotStorage.__init__}")
                replica_manager_instance.hot_storage.print_hot_store()
        else:
            print(f"Error: Unknown command '{statement}'. Use PUT or GET.")