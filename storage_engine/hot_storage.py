class HotStorage:
    def __init__(self):
        self.store = {}

    def put(self, key: str, value_data_pb2):
        self.store[key] = value_data_pb2

    def get(self, key: str):
        return self.store.get(key)

    def remove(self, key: str):
        if key in self.store:
            del self.store[key]
            return True
        return False

    def get_all_keys(self):
        return list(self.store.keys())
    
    def print_hot_store(self):
        if not self.store:
            print("Hot data is empty")
        
        for key, value in self.store.items():
            print(f"'{key}': '{value}'")