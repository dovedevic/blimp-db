import math
from collections import defaultdict

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

# Build the hash table.
for (b_k, b_10, b_100) in db.b:
    if b_100 < sel:
        ht.insert(b_k, b_10)

# Save the hash table.
ht.save('./sq4.json')

# Probe the hash table and compute the result.
result_dict = defaultdict(int)
for (_, a_b_k, a_10, _) in db.a:
    kv = ht.fetch(a_b_k)
    if kv is not None:
        result_dict[kv.val] += a_10
result = list(sorted(result_dict.items()))

# Compare the result to a reference result.
reference_dict = {b_k: b_10 for (b_k, b_10, b_100) in db.b if b_100 < sel}
reference_result_dict = defaultdict(int)
for (_, a_b_k, a_10, _) in db.a:
    value = reference_dict.get(a_b_k)
    if value is not None:
        reference_result_dict[value] += a_10
reference_result = list(sorted(reference_result_dict.items()))
assert result == reference_result, "result does not equal the reference result"
