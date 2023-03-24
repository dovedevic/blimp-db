from typing import Optional

from src.simulators.hashmap import GenericHashTableValue, GenericHashTableValuePayload, GenericHashTableObject, \
    GenericHashTableBucket, GenericHashMap


# Define 32bit numbers
class Object32bit(GenericHashTableValue):
    _SIZE_BYTES = 4


# Define 8bit numbers
class Object8bit(GenericHashTableValue):
    _SIZE_BYTES = 1


# Define 24bit numbers
class Object24bit(GenericHashTableValue):
    _SIZE_BYTES = 3
    _NULL_VALUE = 2 ** (_SIZE_BYTES * 8) - 1


# Define a null payload
class NullPayload(GenericHashTableValuePayload):
    _PAYLOAD_OBJECTS = []


# Define 32bit key, null payload objects
class Hash32bitObjectNullPayload(GenericHashTableObject[Object32bit, NullPayload]):
    _KEY_OBJECT = Object32bit
    _PAYLOAD_OBJECT = NullPayload


# Define a bucket as a collection of 31 Hash32bitObjectNullPayload, with 16bit metadata
class BlimpBucket(GenericHashTableBucket[Hash32bitObjectNullPayload, Object24bit, Object8bit]):
    _KEY_PAYLOAD_OBJECT = Hash32bitObjectNullPayload
    _BUCKET_OBJECT_CAPACITY = 31
    _META_NEXT_BUCKET_OBJECT = Object24bit
    _META_ACTIVE_COUNT_OBJECT = Object8bit


# Define a hash set that uses BlimpBuckets
class BlimpSimpleHashSet(GenericHashMap[BlimpBucket]):
    _BUCKET_OBJECT = BlimpBucket

    def __init__(self, initial_buckets: int, maximum_buckets: int, **kwargs):
        super().__init__(
            initial_buckets,
            maximum_buckets,
            **kwargs
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

    def traced_fetch(self, key) -> ([int], [int], Optional[BlimpBucket]):
        bucket_indices, bucket_iterations, fetched = [], [], None
        bucket_index = self._hash(key)

        while True:  # poor man's python do-while
            bucket = self.buckets[bucket_index]  # type: BlimpBucket
            bucket_indices.append(bucket_index)
            index, hit_object = bucket.get_hit_index(key)
            if hit_object is not None:
                bucket_iterations.append(index + 1)
                fetched = hit_object
                break

            bucket_iterations.append(bucket.count)
            if not bucket.is_next_bucket_valid():
                break
            else:
                bucket_index = bucket.next_bucket

        return bucket_indices, bucket_iterations, fetched
