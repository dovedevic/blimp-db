import json

from typing import Iterator, Union, Optional, List, TypeVar, Generic, Tuple, Type


class GenericHashTableValue:
    """Defines a generic pythonic structure of a _SIZE_BYTES value"""
    _SIZE_BYTES = 0
    _NULL_VALUE = 0

    def __init__(self, value: int):
        """
        Defines a generic value of _SIZE_BYTES bytes.

        @param value: The value represented as an unsigned integer with size _SIZE_BYTES
        """
        assert 0 <= value < 2 ** (self._SIZE_BYTES * 8), f"Value given is not representable by {self._SIZE_BYTES} bytes"
        self._value = value

    @classmethod
    def null_object(cls) -> 'GenericHashTableValue':
        """Create this class with the class's NULL VALUE"""
        return cls(cls._NULL_VALUE)

    @classmethod
    def size(cls) -> int:
        """Return the value size in bytes"""
        return cls._SIZE_BYTES

    @property
    def value(self) -> int:
        """The value of this object"""
        return self._value

    @value.setter
    def value(self, val):
        assert 0 <= val < 2 ** (self._SIZE_BYTES * 8), f"Value given is not representable by {self._SIZE_BYTES} bytes"
        self._value = val

    def __repr__(self):
        return f"<{self.__class__.__name__} {self._value}>"


class GenericHashTableValuePayload:
    """Defines a generic pythonic structure of a collection of objects, or a payload"""
    _PAYLOAD_OBJECTS = (
    )  # type: Tuple[GenericHashTableValue]

    def __init__(self, *payloads: Union[GenericHashTableValue, int]):
        """
        Defines a generic payload object that is a collection of Type[GenericHashTableValue] values.

        @param payloads: The GenericHashTableValues that make up the payload. Must conform to _PAYLOAD_OBJECTS typing
        """
        assert len(payloads) == len(self._PAYLOAD_OBJECTS), "Payloads provided does not equal the object definition"
        self._payloads = []
        for idx, payload in enumerate(payloads):
            if isinstance(payload, self._PAYLOAD_OBJECTS[idx]):  # noqa: pycharm
                self._payloads.append(payload)
            elif isinstance(payload, int):
                self._payloads.append(self._PAYLOAD_OBJECTS[idx](payload))  # noqa: pycharm
            else:
                raise RuntimeError(
                    f"Payload at index {idx} is interpretable of {self._PAYLOAD_OBJECTS[idx].__name__} type")

    @classmethod
    def size(cls) -> int:
        """Return the payload's total size in bytes"""
        total = 0
        for payload_object in cls._PAYLOAD_OBJECTS:
            total += payload_object.size()
        return total

    def __getitem__(self, key: int) -> GenericHashTableValue:
        return self._payloads[key]

    def __setitem__(self, key: int, obj: GenericHashTableValue):
        assert isinstance(obj, self._PAYLOAD_OBJECTS[key]), f"Payload is not of {self._PAYLOAD_OBJECTS[key].__name__} type"  # noqa: pycharm
        self._payloads[key] = obj

    def payloads(self) -> Iterator[GenericHashTableValue]:
        """Iterator over all payloads"""
        for payload in self._payloads:
            yield payload

    @classmethod
    def null_object(cls) -> 'GenericHashTableValuePayload':
        """Create this class with each value's NULL VALUE"""
        return cls(*[p.null_object() for p in cls._PAYLOAD_OBJECTS])

    def __repr__(self):
        return f"<{self.__class__.__name__} [{', '.join(str(payload) for payload in self.payloads())}]>"


KEY_TYPE = TypeVar('KEY_TYPE', bound=GenericHashTableValue)
PAYLOAD_TYPE = TypeVar('PAYLOAD_TYPE', bound=GenericHashTableValuePayload)


class GenericHashTableObject(Generic[KEY_TYPE, PAYLOAD_TYPE]):
    """Defines a generic pythonic structure of a hashable storage object"""
    _KEY_OBJECT = GenericHashTableValue
    _PAYLOAD_OBJECT = GenericHashTableValuePayload

    def __init__(
            self,
            key: Union[KEY_TYPE, int],
            payload: Optional[Union[PAYLOAD_TYPE, List[int]]]=None
            ):
        """
        Defines a generic payload object that is a collection of Type[GenericHashTableValue] values.

        @param key: The GenericHashTableValue that represents the key. Must conform to _KEY_OBJECT type
        @param payload: The GenericHashTableValuePayload that represents a payload. Must conform to _PAYLOAD_OBJECT type
        """
        if isinstance(key, self._KEY_OBJECT):
            self._key = key
        elif isinstance(key, int):
            self._key = self._KEY_OBJECT(key)
        else:
            raise RuntimeError(f"The provided key could not be interpreted as a {self._KEY_OBJECT.__name__} type")

        if isinstance(payload, self._PAYLOAD_OBJECT):
            self._payload = payload
        elif payload is None:
            self._payload = self._PAYLOAD_OBJECT()
        elif isinstance(payload, list) and all(isinstance(p, int) for p in payload):
            self._payload = self._PAYLOAD_OBJECT(*payload)
        else:
            raise RuntimeError(
                f"The provided payload could not be interpreted as a {self._PAYLOAD_OBJECT.__name__} type")

    @classmethod
    def key_type(cls) -> Type[KEY_TYPE]:
        """Return the internal key object"""
        return cls._KEY_OBJECT

    @classmethod
    def payload_type(cls) -> Type[PAYLOAD_TYPE]:
        """Return the internal payload object"""
        return cls._PAYLOAD_OBJECT

    @property
    def key(self) -> KEY_TYPE:
        """Return this objects key object"""
        return self._key

    @key.setter
    def key(self, k: KEY_TYPE):
        assert isinstance(k, self._KEY_OBJECT), f"Key is not of {self._KEY_OBJECT.__name__} type"
        self._key = k

    @property
    def payload(self) -> PAYLOAD_TYPE:
        """Return this objects payload object"""
        return self._payload

    @classmethod
    def size(cls) -> int:
        """Return the key-payload's total size in bytes"""
        return cls._KEY_OBJECT.size() + cls._PAYLOAD_OBJECT.size()

    @classmethod
    def null_object(cls) -> 'GenericHashTableObject':
        """Create this class with the key and payload's respective NULL VALUE"""
        return cls(cls._KEY_OBJECT.null_object(), cls._PAYLOAD_OBJECT.null_object())

    def __repr__(self):
        return f"<{self.__class__.__name__} Key: {self.key}, Payload: {self.payload}>"


KEY_PAYLOAD_TYPE = TypeVar('KEY_PAYLOAD_TYPE', bound=GenericHashTableObject)
META_ACTIVE_COUNT_TYPE = TypeVar('META_ACTIVE_COUNT_TYPE', bound=GenericHashTableValue)
META_NEXT_BUCKET_TYPE = TypeVar('META_NEXT_BUCKET_TYPE', bound=GenericHashTableValue)


class GenericHashTableBucket(Generic[KEY_PAYLOAD_TYPE, META_ACTIVE_COUNT_TYPE, META_NEXT_BUCKET_TYPE]):
    """Defines the pythonic structure of a bucket/collection for GenericHashTableObjects"""
    _KEY_PAYLOAD_OBJECT = GenericHashTableObject
    _BUCKET_OBJECT_CAPACITY = 0
    _META_ACTIVE_COUNT_OBJECT = GenericHashTableValue
    _META_NEXT_BUCKET_OBJECT = GenericHashTableValue

    def __init__(self, objects: List[KEY_PAYLOAD_TYPE]=None, active_count: int=0, next_bucket: int=None):
        """
        Defines a bucket for a collection of hash table objects; this is essentially a really complicated way of
        implementing a generic list :/

        @param objects: The initial list of key/payload objects, should none need to be generated.
        @param active_count: The number of valid/active key values in this bucket
        @param next_bucket: An integer pointing to the next bucket, you'll probably use a list index here
        """
        if objects:
            assert len(objects) == self._BUCKET_OBJECT_CAPACITY, "Provided objects do not equal the bucket capacity"
            assert all(isinstance(obj, self._KEY_PAYLOAD_OBJECT) for obj in objects), \
                f"All objects must be of {self._KEY_PAYLOAD_OBJECT.__name__} type"
            self._objects = objects
        else:
            self._objects = [self._KEY_PAYLOAD_OBJECT.null_object() for _ in range(self._BUCKET_OBJECT_CAPACITY)]
        assert 0 <= active_count <= self._BUCKET_OBJECT_CAPACITY, \
            "The bucket's active count cannot be larger than it's capacity"

        self._active_count = self._META_ACTIVE_COUNT_OBJECT(active_count)
        self._next_bucket = self._META_NEXT_BUCKET_OBJECT.null_object() if next_bucket is None else \
            self._META_NEXT_BUCKET_OBJECT(next_bucket)

    @classmethod
    def bucket_object_type(cls) -> Type[KEY_PAYLOAD_TYPE]:
        """Return the internal key-payload object"""
        return cls._KEY_PAYLOAD_OBJECT

    @classmethod
    def size(cls) -> int:
        """Return the bucket's total size in bytes"""
        return cls._BUCKET_OBJECT_CAPACITY * cls._KEY_PAYLOAD_OBJECT.size() + \
            cls._META_ACTIVE_COUNT_OBJECT.size() + cls._META_NEXT_BUCKET_OBJECT.size()

    @classmethod
    def bucket_capacity(cls) -> int:
        """The total number of objects this bucket can hold"""
        return cls._BUCKET_OBJECT_CAPACITY

    @property
    def count(self) -> int:
        """The number of valid/active objects in this bucket"""
        return self._active_count.value

    @count.setter
    def count(self, value: int):
        self._active_count.value = value

    @property
    def next_bucket(self) -> int:
        """An integer pointing to the next bucket, you'll probably use a list index here"""
        return self._next_bucket.value

    @next_bucket.setter
    def next_bucket(self, value: int):
        self._next_bucket.value = value

    def is_next_bucket_valid(self) -> bool:
        """Is the pointer to the next bucket valid?"""
        return self._next_bucket.value != self._next_bucket._NULL_VALUE

    def set_object(self, index: int, obj: KEY_PAYLOAD_TYPE, increment_count: bool=False):
        """Set the bucket object at a provided index to a specified value."""
        self._objects[index] = obj
        if increment_count:
            self.count += 1

    def add_object(self, obj: KEY_PAYLOAD_TYPE):
        """Add a provided GenericHashTableObject to the end of this bucket, update meta-data"""
        self.set_object(self.count, obj, increment_count=True)

    def get_object(self, index: int) -> KEY_PAYLOAD_TYPE:
        """Fetch a GenericHashTableObject from the bucket via it's index"""
        return self._objects[index]

    def get_hit_index(self, key: int) -> (int, Optional[KEY_PAYLOAD_TYPE]):
        """
        Attempt to locate the provided key in this bucket. If found, will return the index of the hit and the object
        within this bucket. If not found, will return the index of -1 and None type.
        """
        for index, obj in zip(range(self.count), self._objects):
            if key == obj.key.value:
                return index, obj
        return -1, None

    def objects(self) -> Iterator[KEY_PAYLOAD_TYPE]:
        """Iterator over all objects"""
        for obj in self._objects:
            yield obj

    def __repr__(self):
        return f"<{self.__class__.__name__} count:{self.count}, cap: {self._BUCKET_OBJECT_CAPACITY}" \
               f" next_bucket: {self.next_bucket} objects: [{', '.join([str(obj) for obj in self._objects])}]>"


BUCKET_TYPE = TypeVar('BUCKET_TYPE', bound=GenericHashTableBucket)


class GenericHashMap(Generic[BUCKET_TYPE]):
    """Defines a generic collection of buckets for a defined hash-set/map/table"""
    _BUCKET_OBJECT = GenericHashTableBucket

    def __init__(self, initial_buckets: int, maximum_buckets: int, buckets: List[BUCKET_TYPE] = None):
        """
        Defines the pythonic structure of an in-memory hash table using buckets.

        @param initial_buckets: The initial/minimum number bucket objects. Typically, this is load factor dependent
        @param maximum_buckets: The maximum number of bucket objects that are able to be created.
        @param buckets: The initial list of bucket objects, should none need to be generated.
        """

        assert 0 <= initial_buckets <= maximum_buckets, "Initial number of buckets is greater than the maximum capacity"
        self._initial_buckets = initial_buckets
        self._maximum_buckets = maximum_buckets

        if buckets:
            assert len(buckets) <= maximum_buckets, "Provided number of buckets is larger than the maximum capacity"
            assert len(buckets) >= initial_buckets, "Provided number of buckets is less than the initial capacity"
            self.buckets = buckets
        else:
            self.buckets = [self._BUCKET_OBJECT() for _ in range(initial_buckets)]

    @classmethod
    def bucket_type(cls) -> Type[BUCKET_TYPE]:
        """Return the internal bucket object"""
        return cls._BUCKET_OBJECT

    def _hash(self, key: int) -> int:
        """Hash the key and return an index into buckets. The result of this must safely index into the buckets list"""
        raise NotImplementedError

    @property
    def maximum_size(self) -> int:
        """The maximum size of the hash map object, in bytes"""
        return self._maximum_buckets * self._BUCKET_OBJECT.size()

    @property
    def initial_buckets(self) -> int:
        """The number of initial buckets for this hash table"""
        return self._initial_buckets

    @property
    def maximum_buckets(self) -> int:
        """The maximum number of buckets for this hash table"""
        return self._maximum_buckets

    @property
    def size(self) -> int:
        """The current size of the hash map object, in bytes"""
        return len(self.buckets) * self._BUCKET_OBJECT.size()

    def insert(self, obj: GenericHashTableObject):
        """Insert a key/value into a bucket via a hash, creates new buckets as necessary"""
        bucket_hash_index = self._hash(obj.key.value)
        bucket = self.buckets[bucket_hash_index]
        while bucket.is_next_bucket_valid():
            bucket = self.buckets[bucket.next_bucket]

        if bucket.count == bucket.bucket_capacity():  # have we filled the bucket?
            if len(self.buckets) == self._maximum_buckets:  # have we filled our allotment of buckets?
                raise RuntimeError("hashmap capacity exceeded")
            new_bucket = self._BUCKET_OBJECT()
            bucket.next_bucket = len(self.buckets)
            self.buckets.append(new_bucket)
            bucket = new_bucket

        bucket.add_object(obj)

    def fetch(self, key: int) -> Optional[GenericHashTableObject]:
        """
        Fetch the object from the hash map by its key, and traverse buckets as necessary. If not found, returns None
        """
        bucket = self.buckets[self._hash(key)]
        while True:  # poor man's python do-while
            index_in, index_obj = bucket.get_hit_index(key)
            if index_obj is not None:
                return index_obj
            if not bucket.is_next_bucket_valid():
                return None
            else:
                bucket = self.buckets[bucket.next_bucket]

    def save(self, path: str, compact=True):
        """
        Save the hashmap as a JSON object

        @param path: The path and filename to save the configuration
        @param compact: Whether the JSON saved should be compact or indented
        """

        save_dict = {
            'initial_buckets': self._initial_buckets,
            'maximum_buckets': self._maximum_buckets,
            'buckets': [{
                'active_count': bucket.count,
                'next_bucket': bucket.next_bucket,
                'objects': [{
                    'k': obj.key.value,
                    'payload': [p.value for p in obj.payload.payloads()]
                } for obj in bucket.objects()]
            } for bucket in self.buckets]
        }

        with open(path, "w") as fp:
            if compact:
                json.dump(save_dict, fp)
            else:
                json.dump(save_dict, fp, indent=4)

    @classmethod
    def load(cls, path: str):
        """
        Load a hashmap JSON object

        @param path: The path and filename to load the hashmap
        """
        with open(path, 'r') as fp:
            hashobj = json.load(fp)

        return cls(
            initial_buckets=hashobj['initial_buckets'],
            maximum_buckets=hashobj['maximum_buckets'],
            buckets=[cls._BUCKET_OBJECT(
                objects=[
                    cls.bucket_type().bucket_object_type()(
                        obj['k'],
                        obj['payload']
                    ) for obj in bucket['objects']
                ],
                active_count=bucket['active_count'],
                next_bucket=bucket['next_bucket']
            ) for bucket in hashobj['buckets']]
        )
