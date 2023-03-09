from src.simulators.hashmap import *


# Define 32bit KV pairs
class HashTable32KV(GenericHashTableKV):
    """Defines the pythonic structure of a 32-bit key 32-bit value object"""
    _KEY_SIZE_BYTES = 4
    _VALUE_SIZE_BYTES = 4


# Define a bucket using 4 byte count/next bucket values, with UINT32_MAX as the null value
class BlimpHashTableBucket(GenericHashTableBucket[HashTable32KV]):
    _NULL_VALUE = 2 ** 32 - 1
    _ACTIVE_COUNT_SIZE_BYTES = 4
    _NEXT_BUCKET_SIZE_BYTES = 4


# Generate a templated hash table that utilizes 32bit KVs and a simple hash; this is our custom table for BLIMP
class BlimpSimpleHashTable(GenericHashMapObject[HashTable32KV, BlimpHashTableBucket]):
    bucket_capacity = 15

    def __init__(self, initial_buckets: int, maximum_buckets: int):
        super().__init__(
            initial_buckets,
            maximum_buckets,
            bucket_capacity=self.bucket_capacity,
            kv_generator=HashTable32KV,
            bucket_generator=BlimpHashTableBucket
        )

        assert (initial_buckets & (initial_buckets - 1) == 0) and initial_buckets != 0, \
            "initial_buckets must be a power of 2"
        self._mask = initial_buckets - 1

    def _hash(self, key) -> int:
        """Hash the key and return an index into buckets"""
        return (3634946921 * key + 2096170329) & self._mask
