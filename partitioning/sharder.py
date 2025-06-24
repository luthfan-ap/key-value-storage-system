class Sharder:
    def __init__(self, num_shards: int):
        self.num_shards = num_shards

    def get_shard_id(self, key: str) -> int:
        # Menentukan shard ID berdasarkan hash key
        return hash(key) % self.num_shards