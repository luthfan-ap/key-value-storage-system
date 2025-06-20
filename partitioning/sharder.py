# partitioning/sharder.py
class Sharder:
    def __init__(self, num_shards: int):
        self.num_shards = num_shards

    def get_shard_id(self, key: str) -> int:
        return hash(key) % self.num_shards