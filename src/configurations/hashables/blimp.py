from src.simulators.hashmap import GenericHashTableValue, GenericHashTableValuePayload, GenericHashTableObject, \
    GenericHashTableBucket, GenericHashMap


# Define 8bit numbers
class Object8bit(GenericHashTableValue):
    _SIZE_BYTES = 1


# Define 16bit numbers
class Object16bit(GenericHashTableValue):
    _SIZE_BYTES = 2


# Define 24bit numbers
class Object24bit(GenericHashTableValue):
    _SIZE_BYTES = 3


# Define 32bit numbers
class Object32bit(GenericHashTableValue):
    _SIZE_BYTES = 4


# Define 48bit numbers
class Object48bit(GenericHashTableValue):
    _SIZE_BYTES = 6


# Define 64bit numbers
class Object64bit(GenericHashTableValue):
    _SIZE_BYTES = 8


# Define 24bit numbers with 2^24-1 null values
class Object24bitNullMax(Object24bit):
    _NULL_VALUE = 2 ** 24 - 1


# Define 32bit numbers with 2^32-1 null values
class Object32bitNullMax(Object32bit):
    _NULL_VALUE = 2 ** 32 - 1


# Define a null payload
class NullPayload(GenericHashTableValuePayload):
    _PAYLOAD_OBJECTS = []


# Define 32bit key, null payload objects
class Hash32bitObjectNullPayload(GenericHashTableObject[Object32bit, NullPayload]):
    _KEY_OBJECT = Object32bit
    _PAYLOAD_OBJECT = NullPayload


# Define 32bit key, 16b int payload objects
class Hash32bitObject16bPayload(GenericHashTableObject[Object32bit, Object16bit]):
    _KEY_OBJECT = Object32bit
    _PAYLOAD_OBJECT = Object16bit


# Define 32bit key, 8b int payload objects
class Hash32bitObject8bPayload(GenericHashTableObject[Object32bit, Object8bit]):
    _KEY_OBJECT = Object32bit
    _PAYLOAD_OBJECT = Object8bit


# Define a bucket as a collection of 31 Hash32bitObjectNullPayload, with 16bit metadata
class BlimpBucket(GenericHashTableBucket[Hash32bitObjectNullPayload, Object24bitNullMax, Object8bit]):
    _KEY_PAYLOAD_OBJECT = Hash32bitObjectNullPayload
    _BUCKET_OBJECT_CAPACITY = 31
    _META_NEXT_BUCKET_OBJECT = Object24bitNullMax
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
