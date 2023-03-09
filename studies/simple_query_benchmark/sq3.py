import math

from blimp_hash_table import *
from database import *

n_a = 10 ** 3  # Number of rows in table A.
n_b = 10 ** 3  # Number of rows in table B.
sel = 10  # Selectivity (percentage of rows selected).
lf = 0.5  # Load factor of the hash table (number of rows / number of slots).

# Compute the number of initial buckets from the number of rows, selectivity, and load factor. Round up to the next
# power of two.
initial_buckets = math.ceil(n_b * sel / 100 / lf / BlimpSimpleHashTable.bucket_capacity)
initial_buckets = 1 if initial_buckets == 0 else 2 ** math.ceil(math.log2(initial_buckets))

# Define our hash table.
ht = BlimpSimpleHashTable(
    initial_buckets=initial_buckets,
    maximum_buckets=2 ** 24 // 128,  # Limit ourselves to 16MB worth of 128B buckets
)

# Generate the data.
db = SQBDatabase(n_a, n_b)

# Build the hash table. SQ3 is a semijoin, so we need only the keys from B in the hash table, none of the values.
# Eventually, we can optimize this by building a hash set, rather than a hash table. For now, we insert an arbitrary
# integer (0) into the hash table as the value.
for (b_k, _, b_100) in db.b:
    if b_100 < sel:
        ht.insert(b_k, 0)

# Save the hash table.
ht.save('./sq3.json')

# Probe the hash table and compute the result.
result = sum(a_10 for (_, a_b_k, a_10, _) in db.a if ht.fetch(a_b_k) is not None)

# Compare the result to a reference result.
reference_set = {b_k for (b_k, _, b_100) in db.b if b_100 < sel}
reference_result = sum(a_10 for (_, a_b_k, a_10, _) in db.a if a_b_k in reference_set)
assert result == reference_result, "result does not equal the reference result"
