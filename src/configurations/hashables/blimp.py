from typing import Optional

from src.simulators.hashmap import GenericHashTableKV, GenericHashTableBucket, GenericHashMapObject


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

    def __init__(self, initial_buckets: int, maximum_buckets: int, **kwargs):
        super().__init__(
            initial_buckets,
            maximum_buckets,
            bucket_capacity=self.bucket_capacity,
            kv_generator=HashTable32KV,
            bucket_generator=BlimpHashTableBucket,
            buckets=kwargs.get('buckets', None)
        )

        assert (initial_buckets & (initial_buckets - 1) == 0) and initial_buckets != 0, \
            "initial_buckets must be a power of 2"
        self._mask = initial_buckets - 1

    def _hash(self, key) -> int:
        """Hash the key and return an index into buckets"""
        return (3634946921 * key + 2096170329) & self._mask

    @property
    def mask(self):  # used to get the mask info for vectorized hashing
        return self._mask

    @property
    def kv_size(self):
        return HashTable32KV.kv_size()

    @property
    def bucket_size(self):
        return BlimpHashTableBucket.calculate_bucket_size(self.bucket_capacity, self._kv_generator)

    def traced_fetch(self, key) -> ([int], [int], Optional[BlimpHashTableBucket]):
        bucket_indices, bucket_iterations, fetched = [], [], None
        bucket_index = self._hash(key)

        while True:  # poor man's python do-while
            bucket = self.buckets[bucket_index]  # type: BlimpHashTableBucket
            bucket_indices.append(bucket_index)
            is_in, index_in = bucket.get_index(key)
            if is_in:
                bucket_iterations.append(index_in + 1)
                fetched = bucket.kvs[index_in]
                break

            bucket_iterations.append(bucket.count)
            if bucket.next_bucket == bucket.null_value():
                break
            else:
                bucket_index = bucket.next_bucket

        return bucket_indices, bucket_iterations, fetched
