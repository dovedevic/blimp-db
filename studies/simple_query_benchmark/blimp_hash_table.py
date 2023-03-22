from src.simulators.hashmap import *


# Define 32bit numbers
class Object32bit(GenericHashTableValue):
    _SIZE_BYTES = 4


# Define 16bit numbers
class Object16bit(GenericHashTableValue):
    _SIZE_BYTES = 2


# Define 16bit numbers that have a null value as the max int
class MaxIntNull16bObject(Object16bit):
    _NULL_VALUE = 2 ** (Object16bit._SIZE_BYTES * 8) - 1


# Define a null payload
class NullPayload(GenericHashTableValuePayload):
    _PAYLOAD_OBJECTS = []


# Define 32bit key, null payload objects
class Hash32bitObjectNullPayload(GenericHashTableObject):
    _KEY_OBJECT = Object32bit
    _PAYLOAD_OBJECT = NullPayload


# Define a bucket as a collection of 31 Hash32bitObjectNullPayload, with 16bit metadata
class BlimpBucket(GenericHashTableBucket):
    _KEY_PAYLOAD_OBJECT = Hash32bitObjectNullPayload
    _BUCKET_OBJECT_CAPACITY = 31
    _META_NEXT_BUCKET_OBJECT = MaxIntNull16bObject
    _META_ACTIVE_COUNT_OBJECT = Object16bit


# Define a hash set that uses BlimpBuckets
class BlimpHashSet(GenericHashMap):
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
