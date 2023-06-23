from studies.star_schema_benchmark.queries.q2.q2_1 import \
    SSBQuery2p1BlimpVPartSupplierDate, SSBQuery2p1BlimpPartSupplierDate
from src.configurations.hashables.blimp import BlimpSimpleHashSet, Hash32bitObjectNullPayload, BlimpBucket, \
        Object64bit, Object48bit, Object32bit, Object24bit, Object24bitNullMax, Object16bit, Object8bit, NullPayload
from src.simulators.hashmap import GenericHashTableObject


class Object64bitNullMax(Object64bit):
    _NULL_VALUE = 2 ** 64 - 1


class Object32bitNullMax(Object32bit):
    _NULL_VALUE = 2 ** 32 - 1


class Blimp1Cap32bk16bpHashMap(BlimpSimpleHashSet):
    class Blimp32bk16bpBucket(BlimpBucket):
        class Hash32bitObject16bPayload(GenericHashTableObject[Object32bit, Object16bit]):
            _KEY_OBJECT = Object32bit
            _PAYLOAD_OBJECT = Object16bit

        _KEY_PAYLOAD_OBJECT = Hash32bitObject16bPayload
        _BUCKET_OBJECT_CAPACITY = 1
        _META_NEXT_BUCKET_OBJECT = Object64bitNullMax
        _META_ACTIVE_COUNT_OBJECT = Object16bit

    _BUCKET_OBJECT = Blimp32bk16bpBucket


class Blimp1Cap32bkNbpHashSet(BlimpSimpleHashSet):
    class Blimp32bkNbpBucket(BlimpBucket):
        class Hash32bitObjectNbPayload(GenericHashTableObject[Object32bit, NullPayload]):
            _KEY_OBJECT = Object32bit
            _PAYLOAD_OBJECT = NullPayload

        _KEY_PAYLOAD_OBJECT = Hash32bitObjectNbPayload
        _BUCKET_OBJECT_CAPACITY = 1
        _META_NEXT_BUCKET_OBJECT = Object24bitNullMax
        _META_ACTIVE_COUNT_OBJECT = Object8bit

    _BUCKET_OBJECT = Blimp32bkNbpBucket


class SSB2p1BlimpHash1MaxChain1(SSBQuery2p1BlimpPartSupplierDate):
    supplier_join_hash_table = Blimp1Cap32bkNbpHashSet(262144, 262144)
    part_join_hash_table = Blimp1Cap32bk16bpHashMap(1048576, 2097152)
    date_join_hash_table = Blimp1Cap32bk16bpHashMap(8192, 8192)


class SSB2p1BlimpHash1MaxChain2(SSBQuery2p1BlimpPartSupplierDate):
    supplier_join_hash_table = Blimp1Cap32bkNbpHashSet(131072, 262144)
    part_join_hash_table = Blimp1Cap32bk16bpHashMap(1048576, 2097152)
    date_join_hash_table = Blimp1Cap32bk16bpHashMap(4096, 8192)


class SSB2p1BlimpHash1MaxChain3(SSBQuery2p1BlimpPartSupplierDate):
    supplier_join_hash_table = Blimp1Cap32bkNbpHashSet(65536, 262144)
    part_join_hash_table = Blimp1Cap32bk16bpHashMap(524288, 2097152)
    date_join_hash_table = Blimp1Cap32bk16bpHashMap(2048, 8192)


class SSB2p1BlimpHash1MaxChain4(SSBQuery2p1BlimpPartSupplierDate):
    supplier_join_hash_table = Blimp1Cap32bkNbpHashSet(32768, 262144)
    part_join_hash_table = Blimp1Cap32bk16bpHashMap(262144, 2097152)
    date_join_hash_table = Blimp1Cap32bk16bpHashMap(1024, 8192)


class Blimp2Cap32bk16bpHashMap(BlimpSimpleHashSet):
    class Blimp32bk16bpBucket(BlimpBucket):
        class Hash32bitObject16bPayload(GenericHashTableObject[Object32bit, Object16bit]):
            _KEY_OBJECT = Object32bit
            _PAYLOAD_OBJECT = Object16bit

        _KEY_PAYLOAD_OBJECT = Hash32bitObject16bPayload
        _BUCKET_OBJECT_CAPACITY = 2
        _META_NEXT_BUCKET_OBJECT = Object24bitNullMax
        _META_ACTIVE_COUNT_OBJECT = Object8bit

    _BUCKET_OBJECT = Blimp32bk16bpBucket


class Blimp2Cap32bkNbpHashSet(BlimpSimpleHashSet):
    class Blimp32bkNbpBucket(BlimpBucket):
        class Hash32bitObjectNbPayload(GenericHashTableObject[Object32bit, NullPayload]):
            _KEY_OBJECT = Object32bit
            _PAYLOAD_OBJECT = NullPayload

        _KEY_PAYLOAD_OBJECT = Hash32bitObjectNbPayload
        _BUCKET_OBJECT_CAPACITY = 2
        _META_NEXT_BUCKET_OBJECT = Object32bitNullMax
        _META_ACTIVE_COUNT_OBJECT = Object32bit

    _BUCKET_OBJECT = Blimp32bkNbpBucket


class SSB2p1BlimpHash2MaxChain1(SSBQuery2p1BlimpPartSupplierDate):
    supplier_join_hash_table = Blimp2Cap32bkNbpHashSet(131072, 131072)
    part_join_hash_table = Blimp2Cap32bk16bpHashMap(524288, 1048576)
    date_join_hash_table = Blimp2Cap32bk16bpHashMap(4096, 4096)


class SSB2p1BlimpHash2MaxChain2(SSBQuery2p1BlimpPartSupplierDate):
    supplier_join_hash_table = Blimp2Cap32bkNbpHashSet(65536, 131072)
    part_join_hash_table = Blimp2Cap32bk16bpHashMap(262144, 1048576)
    date_join_hash_table = Blimp2Cap32bk16bpHashMap(2048, 8192)


class SSB2p1BlimpHash2MaxChain3(SSBQuery2p1BlimpPartSupplierDate):
    supplier_join_hash_table = Blimp2Cap32bkNbpHashSet(32768, 131072)
    part_join_hash_table = Blimp2Cap32bk16bpHashMap(131072, 1048576)
    date_join_hash_table = Blimp2Cap32bk16bpHashMap(1024, 4096)


class SSB2p1BlimpHash2MaxChain4(SSBQuery2p1BlimpPartSupplierDate):
    supplier_join_hash_table = Blimp2Cap32bkNbpHashSet(16384, 131072)
    part_join_hash_table = Blimp2Cap32bk16bpHashMap(65536, 1048576)
    date_join_hash_table = Blimp2Cap32bk16bpHashMap(512, 4096)


class Blimp4Cap32bk16bpHashMap(BlimpSimpleHashSet):
    class Blimp32bk16bpBucket(BlimpBucket):
        class Hash32bitObject16bPayload(GenericHashTableObject[Object32bit, Object16bit]):
            _KEY_OBJECT = Object32bit
            _PAYLOAD_OBJECT = Object16bit

        _KEY_PAYLOAD_OBJECT = Hash32bitObject16bPayload
        _BUCKET_OBJECT_CAPACITY = 4
        _META_NEXT_BUCKET_OBJECT = Object32bitNullMax
        _META_ACTIVE_COUNT_OBJECT = Object32bit

    _BUCKET_OBJECT = Blimp32bk16bpBucket


class Blimp4Cap32bkNbpHashSet(BlimpSimpleHashSet):
    class Blimp32bkNbpBucket(BlimpBucket):
        class Hash32bitObjectNbPayload(GenericHashTableObject[Object32bit, NullPayload]):
            _KEY_OBJECT = Object32bit
            _PAYLOAD_OBJECT = NullPayload

        _KEY_PAYLOAD_OBJECT = Hash32bitObjectNbPayload
        _BUCKET_OBJECT_CAPACITY = 4
        _META_NEXT_BUCKET_OBJECT = Object64bitNullMax
        _META_ACTIVE_COUNT_OBJECT = Object64bit

    _BUCKET_OBJECT = Blimp32bkNbpBucket


class SSB2p1BlimpHash4MaxChain1(SSBQuery2p1BlimpPartSupplierDate):
    supplier_join_hash_table = Blimp4Cap32bkNbpHashSet(65536, 65536)
    part_join_hash_table = Blimp4Cap32bk16bpHashMap(262144, 262144)
    date_join_hash_table = Blimp4Cap32bk16bpHashMap(2048, 2048)


class SSB2p1BlimpHash4MaxChain2(SSBQuery2p1BlimpPartSupplierDate):
    supplier_join_hash_table = Blimp4Cap32bkNbpHashSet(16384, 65536)
    part_join_hash_table = Blimp4Cap32bk16bpHashMap(65536, 262144)
    date_join_hash_table = Blimp4Cap32bk16bpHashMap(512, 2048)


class SSB2p1BlimpHash4MaxChain3(SSBQuery2p1BlimpPartSupplierDate):
    supplier_join_hash_table = Blimp4Cap32bkNbpHashSet(8192, 65536)
    part_join_hash_table = Blimp4Cap32bk16bpHashMap(32768, 262144)
    date_join_hash_table = Blimp4Cap32bk16bpHashMap(256, 2048)


class SSB2p1BlimpHash4MaxChain4(SSBQuery2p1BlimpPartSupplierDate):
    supplier_join_hash_table = Blimp4Cap32bkNbpHashSet(4096, 65536)
    part_join_hash_table = Blimp4Cap32bk16bpHashMap(16384, 262144)
    date_join_hash_table = Blimp4Cap32bk16bpHashMap(128, 2048)


class Blimp8Cap32bk16bpHashMap(BlimpSimpleHashSet):
    class Blimp32bk16bpBucket(BlimpBucket):
        class Hash32bitObject16bPayload(GenericHashTableObject[Object32bit, Object16bit]):
            _KEY_OBJECT = Object32bit
            _PAYLOAD_OBJECT = Object16bit

        _KEY_PAYLOAD_OBJECT = Hash32bitObject16bPayload
        _BUCKET_OBJECT_CAPACITY = 8
        _META_NEXT_BUCKET_OBJECT = Object64bitNullMax
        _META_ACTIVE_COUNT_OBJECT = Object64bit

    _BUCKET_OBJECT = Blimp32bk16bpBucket


class Blimp8Cap32bkNbpHashSet(BlimpSimpleHashSet):
    class Blimp32bkNbpBucket(BlimpBucket):
        class Hash32bitObjectNbPayload(GenericHashTableObject[Object32bit, NullPayload]):
            _KEY_OBJECT = Object32bit
            _PAYLOAD_OBJECT = NullPayload

        _KEY_PAYLOAD_OBJECT = Hash32bitObjectNbPayload
        _BUCKET_OBJECT_CAPACITY = 7
        _META_NEXT_BUCKET_OBJECT = Object24bitNullMax
        _META_ACTIVE_COUNT_OBJECT = Object8bit

    _BUCKET_OBJECT = Blimp32bkNbpBucket


class SSB2p1BlimpHash8MaxChain0(SSBQuery2p1BlimpPartSupplierDate):
    supplier_join_hash_table = Blimp8Cap32bkNbpHashSet(32768, 32768)
    part_join_hash_table = Blimp8Cap32bk16bpHashMap(131072, 131072)
    date_join_hash_table = Blimp8Cap32bk16bpHashMap(1024, 1024)


class SSB2p1BlimpHash8MaxChain1(SSBQuery2p1BlimpPartSupplierDate):
    supplier_join_hash_table = Blimp8Cap32bkNbpHashSet(16384, 32768)
    part_join_hash_table = Blimp8Cap32bk16bpHashMap(65536, 65536)
    date_join_hash_table = Blimp8Cap32bk16bpHashMap(512, 512)


class SSB2p1BlimpHash8MaxChain2(SSBQuery2p1BlimpPartSupplierDate):
    supplier_join_hash_table = Blimp8Cap32bkNbpHashSet(8192, 16384)
    part_join_hash_table = Blimp8Cap32bk16bpHashMap(32768, 65536)
    date_join_hash_table = Blimp8Cap32bk16bpHashMap(256, 512)


class SSB2p1BlimpHash8MaxChain3(SSBQuery2p1BlimpPartSupplierDate):
    supplier_join_hash_table = Blimp8Cap32bkNbpHashSet(4096, 16384)
    part_join_hash_table = Blimp8Cap32bk16bpHashMap(16384, 65536)
    date_join_hash_table = Blimp8Cap32bk16bpHashMap(128, 512)


class SSB2p1BlimpHash8MaxChain4(SSBQuery2p1BlimpPartSupplierDate):
    supplier_join_hash_table = Blimp8Cap32bkNbpHashSet(2048, 16384)
    part_join_hash_table = Blimp8Cap32bk16bpHashMap(8192, 65536)
    date_join_hash_table = Blimp8Cap32bk16bpHashMap(64, 512)


class Blimp16Cap32bk16bpHashMap(BlimpSimpleHashSet):
    class Blimp32bk16bpBucket(BlimpBucket):
        class Hash32bitObject16bPayload(GenericHashTableObject[Object32bit, Object16bit]):
            _KEY_OBJECT = Object32bit
            _PAYLOAD_OBJECT = Object16bit

        _KEY_PAYLOAD_OBJECT = Hash32bitObject16bPayload
        _BUCKET_OBJECT_CAPACITY = 19
        _META_NEXT_BUCKET_OBJECT = Object64bitNullMax
        _META_ACTIVE_COUNT_OBJECT = Object48bit

    _BUCKET_OBJECT = Blimp32bk16bpBucket


class Blimp16Cap32bkNbpHashSet(BlimpSimpleHashSet):
    class Blimp32bkNbpBucket(BlimpBucket):
        class Hash32bitObjectNbPayload(GenericHashTableObject[Object32bit, NullPayload]):
            _KEY_OBJECT = Object32bit
            _PAYLOAD_OBJECT = NullPayload

        _KEY_PAYLOAD_OBJECT = Hash32bitObjectNbPayload
        _BUCKET_OBJECT_CAPACITY = 29
        _META_NEXT_BUCKET_OBJECT = Object64bitNullMax
        _META_ACTIVE_COUNT_OBJECT = Object32bit

    _BUCKET_OBJECT = Blimp32bkNbpBucket


class SSB2p1BlimpHash16MaxChain1(SSBQuery2p1BlimpPartSupplierDate):
    supplier_join_hash_table = Blimp16Cap32bkNbpHashSet(32768, 32768)
    part_join_hash_table = Blimp16Cap32bk16bpHashMap(131072, 131072)
    date_join_hash_table = Blimp16Cap32bk16bpHashMap(1024, 1024)


class SSB2p1BlimpHash16MaxChain2(SSBQuery2p1BlimpPartSupplierDate):
    supplier_join_hash_table = Blimp16Cap32bkNbpHashSet(2048, 32768)
    part_join_hash_table = Blimp16Cap32bk16bpHashMap(4096, 131072)
    date_join_hash_table = Blimp16Cap32bk16bpHashMap(128, 1024)


# test
# a = SSB2p1BlimpHash16MaxChain2()
# a._setup()
# a.supplier_join_hash_table.get_statistics(display=True)
# a.part_join_hash_table.get_statistics(display=True)
# a.date_join_hash_table.get_statistics(display=True)
# exit()


def get_times(queries: [SSBQuery2p1BlimpPartSupplierDate]):
    for query in queries:
        query_obj = query()
        _, runtimes = query_obj.run_query(display_runtime_output=False)
        print(
            str(query_obj.__class__.__name__),
            sum([r.runtime for r in runtimes]),
            str(
                query_obj.date_join_hash_table.get_statistics()["size"] +
                query_obj.supplier_join_hash_table.get_statistics()["size"] +
                query_obj.part_join_hash_table.get_statistics()["size"]
            ),
            sep='\t',
        )


get_times([
    SSB2p1BlimpHash1MaxChain1,
    SSB2p1BlimpHash1MaxChain2,
    SSB2p1BlimpHash1MaxChain3,
    SSB2p1BlimpHash1MaxChain4,
    SSB2p1BlimpHash2MaxChain1,
    SSB2p1BlimpHash2MaxChain2,
    SSB2p1BlimpHash2MaxChain3,
    SSB2p1BlimpHash2MaxChain4,
    SSB2p1BlimpHash4MaxChain1,
    SSB2p1BlimpHash4MaxChain2,
    SSB2p1BlimpHash4MaxChain3,
    SSB2p1BlimpHash4MaxChain4,
    SSB2p1BlimpHash8MaxChain0,
    SSB2p1BlimpHash8MaxChain1,
    SSB2p1BlimpHash8MaxChain2,
    SSB2p1BlimpHash8MaxChain3,
    SSB2p1BlimpHash8MaxChain4,
    SSB2p1BlimpHash16MaxChain1,
    SSB2p1BlimpHash16MaxChain2,
    SSBQuery2p1BlimpPartSupplierDate,  # base case
])
