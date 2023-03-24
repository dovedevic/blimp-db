import math

from blimp_hash_table import *
from database import *


def run_sq3(
        n_a: int,
        n_b: int,
        selectivity: int,
        load_factor: float,
        hash_table_size_bytes: int=2**22,
        save_hash_table: str=None
):
    """

    @param n_a: Number of rows in table A
    @param n_b: Number of rows in table B
    @param selectivity: Selectivity (percentage of rows selected)
    @param load_factor: Load factor of the hash table (number of rows / number of slots)
    @param hash_table_size_bytes: The maximum size allowed for the hash table. Defaults to 4MB
    @param save_hash_table: The file-path to save the built hash table to. Leave empty to not save anything.
    """

    # Compute the number of initial buckets from the number of rows, selectivity, and load factor. Round up to the next
    # power of two.
    initial_buckets = math.ceil(n_b * selectivity / 100 / load_factor / BlimpBucket._BUCKET_OBJECT_CAPACITY)
    initial_buckets = 1 if initial_buckets == 0 else 2 ** math.ceil(math.log2(initial_buckets))

    # Define our hash set.
    ht = BlimpHashSet(
        initial_buckets=initial_buckets,
        maximum_buckets=hash_table_size_bytes // BlimpBucket.size()
    )

    # Generate the data.
    db = SQBDatabase(n_a, n_b)

    # Build the hash table. SQ3 is a semijoin, so we need only the keys from B in the hash table, none of the values.
    # Eventually, we can optimize this by building a hash set, rather than a hash table. For now, we insert an arbitrary
    # integer (0) into the hash table as the value.
    for (b_k, _, b_100) in db.b:
        if b_100 < selectivity:
            ht.insert(Hash32bitObjectNullPayload(b_k))

    # Display some metrics
    print("hash table initial size", ht.initial_buckets * 128)  # initial size
    print("hash table current size", ht.size)
    print("hash table added buckets", (ht.size - ht.initial_buckets * 128) // 128)
    print("hash table maximum size", ht.maximum_size)

    # Save the hash table.
    if save_hash_table:
        ht.save(save_hash_table)

    # Probe the hash table and compute the result.
    result = sum(a_10 for (_, a_b_k, a_10, _) in db.a if ht.fetch(a_b_k) is not None)

    # Compare the result to a reference result.
    reference_set = {b_k for (b_k, _, b_100) in db.b if b_100 < selectivity}
    reference_result = sum(a_10 for (_, a_b_k, a_10, _) in db.a if a_b_k in reference_set)
    assert result == reference_result, "result does not equal the reference result"


if __name__ == '__main__':
    run_sq3(
        n_a=2**21,
        n_b=2**24,
        selectivity=18,
        load_factor=0.8,
        hash_table_size_bytes=2**25,
        save_hash_table=f'./sq3.{2**21}.{2**24}.{18}.08.16MB.json'
    )
