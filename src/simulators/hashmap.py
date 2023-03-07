import json

from typing import Tuple, List, Generic, TypeVar, Type, Optional


class GenericHashTableKV:
    """Defines a generic pythonic structure of a KV storage object"""
    _KEY_SIZE_BYTES = None
    _VALUE_SIZE_BYTES = None
    _NULL_VALUE = 0

    def __init__(self, k=None, v=None, kv=None):
        """
        Defines a Key-Value pair with class-defined KEY_SIZE_BYTES keys and VALUE_SIZE_BYTES values.

        @param k: The key represented as an unsigned integer with size KEY_SIZE_BYTES
        @param v: The value represented as an unsigned integer with size VALUE_SIZE_BYTES
        @param kv: The key-value pair represented as an unsigned integers. Providing this argument will override what is
                   provided by @param k and @param v. The value will be interpreted as a packed join of kv.
        """

        if kv is not None:
            assert 0 <= kv < 2 ** (self.kv_size() * 8), f"KV pair given is not representable by {self.kv_size()} bytes"
            k = (kv & (((2 ** (self.key_size() * 8)) - 1) << (self.value_size() * 8))) >> (self.value_size() * 8)
            v = kv & ((2 ** (self.value_size() * 8)) - 1)
        if k is not None:
            assert 0 <= k < 2 ** (self.key_size() * 8), \
                f"Key given is not representable by {self.key_size()} bytes"
        if v is not None:
            assert 0 <= v < 2 ** (self.value_size() * 8), \
                f"Value given is not representable by {self.value_size()} bytes"
        self._k = self.null_value() if k is None else k
        self._v = self.null_value() if v is None else v

    @property
    def key(self):
        return self._k

    @key.setter
    def key(self, value):
        assert 0 <= value < 2 ** (self.key_size() * 8), \
            f"Key given `{value}` is not representable by {self.key_size()} bytes"
        self._k = value

    @property
    def val(self):
        return self._v

    @val.setter
    def val(self, value):
        assert 0 <= value < 2 ** (self.value_size() * 8), \
            f"Value given `{value}` is not representable by {self.value_size()} bytes"
        self._v = value

    @classmethod
    def null_value(cls) -> int:
        """The value used for uninitialized or null memory/keys/values"""
        return cls._NULL_VALUE

    @classmethod
    def key_size(cls) -> int:
        """The key size, in bytes"""
        return cls._KEY_SIZE_BYTES

    @classmethod
    def value_size(cls) -> int:
        """The value size, in bytes"""
        return cls._VALUE_SIZE_BYTES

    @classmethod
    def kv_size(cls) -> int:
        """The total key value size when packed, in bytes"""
        return cls._KEY_SIZE_BYTES + cls._VALUE_SIZE_BYTES

    def as_int(self) -> int:
        """Return the KV object as an unbounded integer"""
        return (self.key << self.value_size()) + self.val

    @classmethod
    def from_int(cls, kv: int) -> object:
        """Return this class interpreted from the provided kv integer"""
        return cls(kv=kv)

    def __repr__(self):
        return f"<{self.__class__.__name__} k:{self.key}, v: {self.val}>"


KV_TYPE = TypeVar('KV_TYPE', bound=GenericHashTableKV)


class GenericHashTableBucket(Generic[KV_TYPE]):
    """Defines the pythonic structure of a collection of generic key-value pairs"""
    _NULL_VALUE = 0
    _ACTIVE_COUNT_SIZE_BYTES = 0
    _NEXT_BUCKET_SIZE_BYTES = 0

    def __init__(self, bucket_kv_capacity: int, kv_generator: Type[KV_TYPE] = None, kvs: Tuple[KV_TYPE]=None,
                 active_count: int=0, next_bucket: int=None):
        """
        Defines a bucket for a collection of KVs; this is essentially a complicated way of implementing a list :/

        @param bucket_kv_capacity: The number of key value pairs allowed in this bucket
        @param kv_generator: The class generator or object to generate key value pairs up to `bucket_kv_capacity`
        @param kvs: The initial list of key-values, should none be generated.
        @param active_count: The number of valid/active key values in this bucket
        @param next_bucket: An integer pointing to the next bucket, you'll probably use a list index here
        """
        self._bucket_kv_capacity = bucket_kv_capacity

        if kvs:
            assert len(kvs) == bucket_kv_capacity, "Provided KVs does not equal the bucket capacity"
            self.kvs = kvs
        else:
            self.kvs = [kv_generator() for _ in range(bucket_kv_capacity)]

        # Jank, but forces the check on the setter
        self.count = active_count
        self.next_bucket = self.null_value() if next_bucket is None else next_bucket

    @classmethod
    def null_value(cls) -> int:
        """The value used for uninitialized or null memory/keys/values"""
        return cls._NULL_VALUE

    @classmethod
    def _active_count_size(cls) -> int:
        """The active count int size, in bytes"""
        return cls._ACTIVE_COUNT_SIZE_BYTES

    @classmethod
    def _next_bucket_size(cls) -> int:
        """The next bucket int size, in bytes"""
        return cls._NEXT_BUCKET_SIZE_BYTES

    @property
    def bucket_kv_capacity(self):
        """The number of key value pairs allowed in this bucket"""
        return self._bucket_kv_capacity

    @property
    def count(self):
        """The number of valid/active key values in this bucket"""
        return self._active_count

    @count.setter
    def count(self, c):
        """The number of valid/active key values in this bucket"""
        assert 0 <= c < 2 ** (self._active_count_size() * 8)
        self._active_count = c

    @property
    def next_bucket(self):
        """An integer pointing to the next bucket, you'll probably use a list index here"""
        return self._next_bucket

    @next_bucket.setter
    def next_bucket(self, n):
        """An integer pointing to the next bucket, you'll probably use a list index here"""
        assert 0 <= n < 2 ** (self._next_bucket_size() * 8)
        self._next_bucket = n

    def bucket_size(self) -> int:
        """The total size of the bucket when packed, in bytes. This includes KVs and bucket metadata"""
        return KV_TYPE.kv_size() * self._bucket_kv_capacity + \
            self._active_count_size() + self._next_bucket_size()

    @classmethod
    def calculate_bucket_size(cls, bucket_kv_capacity) -> int:
        """The total size of the bucket when packed, in bytes. This includes KVs and bucket metadata"""
        return KV_TYPE.kv_size() * bucket_kv_capacity + \
            cls._active_count_size() + cls._next_bucket_size()

    def set_kv(self, index, key: int=None, value: int=None, kv: KV_TYPE=None, increment_count=True):
        """Set the kv at the provided index to a specified key-value or use a provided kv object."""
        self.kvs[index].key = kv.key if kv is not None else key
        self.kvs[index].val = kv.val if kv is not None else value
        if increment_count:
            self.count += 1

    def reset_kv(self, index, decrement_count=True):
        """Set the kv at the provided index to kv object's defined null value."""
        self.kvs[index].key = KV_TYPE.null_value()
        self.kvs[index].val = KV_TYPE.null_value()
        if decrement_count:
            self.count -= 1

    def get_index(self, key: int) -> (bool, int):
        """
        Attempt to locate the provided key in this bucket. If found, will return a hit of true and the index of the hit
        within this bucket. If not found, will return a hit of false and the index of -1.
        """
        for index, kv in enumerate(self.kvs):
            if key == kv.key:
                return True, index
        return False, -1

    def as_int(self) -> int:
        value = 0
        for kv in self.kvs:
            value <<= kv.kv_size()
            value += kv.as_int()

        value <<= self._active_count_size()
        value += self.count
        value <<= self._next_bucket_size()
        value += self.next_bucket

        return value

    def __repr__(self):
        return f"<{self.__class__.__name__} count:{self.count}, cap: {self._bucket_kv_capacity}" \
               f" next_bucket: {self.next_bucket} kvs: [{repr(self.kvs)}]>"


BUCKET_TYPE = TypeVar('BUCKET_TYPE', bound=GenericHashTableBucket)


class GenericHashMapObject(Generic[KV_TYPE, BUCKET_TYPE]):
    """
    Defines the pythonic structure of an in-memory hash table using buckets.

    @param initial_buckets: Initial number of buckets to preallocate, **must be a power of two**
    @param maximum_buckets: The total number of buckets that can exist.
    @param bucket_capacity: The number of key value pairs allowed in any bucket
    @param kv_generator: The class generator or object to generate key value pairs for new buckets
    @param bucket_generator: The class generator or object to generate new buckets
    """
    def __init__(self, initial_buckets: int, maximum_buckets: int, bucket_capacity: int, kv_generator: Type[KV_TYPE],
                 bucket_generator: Type[BUCKET_TYPE], buckets: List[BUCKET_TYPE] = None):
        self._initial_buckets = initial_buckets
        self._maximum_buckets = maximum_buckets
        self._bucket_capacity = bucket_capacity
        self._kv_generator = kv_generator
        self._bucket_generator = bucket_generator

        if buckets:
            assert len(buckets) <= maximum_buckets, "Provided buckets are larger than the maximum capacity"
            assert len(buckets) >= initial_buckets, "Provided buckets is smaller than the initial capacity"
            self.buckets = buckets
        else:
            self.buckets = [bucket_generator(bucket_capacity, kv_generator) for _ in range(initial_buckets)]

    def _hash(self, key: int) -> int:
        """Hash the key and return an index into buckets"""
        raise NotImplementedError

    def insert(self, k: int, v: int):
        """Insert a key/value into a bucket via a hash, creates new buckets as necessary"""
        bucket_hash_index = self._hash(k)
        bucket = self.buckets[bucket_hash_index]
        while bucket.next_bucket != bucket.null_value():
            bucket = self.buckets[bucket.next_bucket]

        if bucket.count == bucket.bucket_kv_capacity:  # have we filled the bucket?
            if len(self.buckets) == self._maximum_buckets:  # have we filled our allotment of buckets?
                raise RuntimeError("hashmap capacity exceeded")
            new_bucket = self._bucket_generator(self._bucket_capacity, self._kv_generator)
            bucket.next_bucket = len(self.buckets)
            self.buckets.append(new_bucket)
            bucket = new_bucket

        bucket.set_kv(bucket.count, key=k, value=v)

    def fetch(self, key: int) -> Optional[GenericHashTableKV]:
        """Fetch the key from the hash map, traverses buckets as necessary. If it does not exist, returns None"""
        bucket = self.buckets[self._hash(key)]
        while True:  # poor man's python do-while
            is_in, index_in = bucket.get_index(key)
            if is_in:
                return bucket.kvs[index_in]
            if bucket.next_bucket == bucket.null_value():
                return None
            else:
                bucket = self.buckets[bucket.next_bucket]

    def save(self, path: str, compact=False):
        """
        Save the hashmap as a JSON object

        @param path: The path and filename to save the configuration
        @param compact: Whether the JSON saved should be compact or indented
        """

        save_dict = {
            'initial_buckets': self._initial_buckets,
            'maximum_buckets': self._maximum_buckets,
            'bucket_capacity': self._bucket_capacity,
            'buckets': [{
                'active_count': bucket.count,
                'next_bucket': bucket.next_bucket,
                'kvs': [{
                    'k': kv.key,
                    'v': kv.val
                } for kv in bucket.kvs]
            } for bucket in self.buckets]
        }

        with open(path, "w") as fp:
            if compact:
                json.dump(save_dict, fp)
            else:
                json.dump(save_dict, fp, indent=4)

    @classmethod
    def load(cls, path: str, kv_generator: Type[KV_TYPE], bucket_generator: Type[BUCKET_TYPE]):
        """
        Load a hashmap JSON object

        @param path: The path and filename to load the hashmap
        @param kv_generator: The class generator or object to generate key value pairs for new buckets
        @param bucket_generator: The class generator or object to generate new buckets
        """
        with open(path, 'r') as fp:
            hashobj = json.load(fp)

        return cls(
            initial_buckets=hashobj['initial_buckets'],
            maximum_buckets=hashobj['maximum_buckets'],
            bucket_capacity=hashobj['bucket_capacity'],
            kv_generator=kv_generator,
            bucket_generator=bucket_generator,
            buckets=[bucket_generator(
                bucket_kv_capacity=hashobj['bucket_capacity'],
                kv_generator=kv_generator,
                kvs=tuple(
                    [
                        kv_generator(
                            k=kv['k'],
                            v=kv['v'],
                        ) for kv in bucket['kvs']
                    ]
                ),
                active_count=bucket['active_count'],
                next_bucket=bucket['next_bucket']
            ) for bucket in hashobj['buckets']]
        )
