from .api import KeyValueStore
import time
import sys

if __name__ == "__main__":
    store = KeyValueStore(num_shards=2)

    # Mode operasi CLI
    print("== Key-Value Storage System ==")
    print("1. Input data (Usage: PUT <key> <value>)")
    print("2. Get data (Usage: GET <key>)\n")
    while True:
        user_input = input("> ").strip()
        parts = user_input.split(maxsplit=2)

        statement = parts[0].upper() # first argument, (PUT / GET)

        if statement == "PUT":
            if len(parts) >= 3: # argument template: python -m ... PUT <key> <value>
                key = parts[1] # second argument, key
                
                value = " ".join(parts[2:])
                store.put(key, value)
                print(f"PUT command executed for key: '{key}'")
            else:
                print("Usage: PUT <key> <value>")
        elif statement == "GET":
            if len(parts) == 2:
                key = parts[1]
                result = store.get(key)
                print(f"Result for '{key}': {result}")
            else:
                print("Usage: GET <key>")
        elif statement == ".QUIT": # if done, do a .quit to shutdown the storage system
            print("\n--- Shutting down store (flushing hot data) ---")
            store.shutdown()
            break
        else:
            print(f"Error: Unknown command '{statement}'. Use PUT or GET.")