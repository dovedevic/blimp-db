# Microbenchmarks

## Data

### Table `A`

| Column  | Domain                                           |
| ------- | ------------------------------------------------ |
| `a_k`   | $\left[ 0, \lvert A \rvert - 1 \right]$ (unique) |
| `a_b_k` | $\left[ 0, \lvert B \rvert - 1 \right]$          |
| `a_10`  | $\left[ 0, 9 \right]$                            |
| `a_100` | $\left[ 0, 99 \right]$                           |

### Table `B`

| Column  | Domain                                           |
| ------- | ------------------------------------------------ |
| `b_k`   | $\left[ 0, \lvert B \rvert - 1 \right]$ (unique) |
| `b_10`  | $\left[ 0, 9 \right]$                            |
| `b_100` | $\left[ 0, 99 \right]$                           |

- All columns are 32-bit integers.
- `A` has $1 \times 10^8$ rows.
- `B` has $1 \times 10^6$ rows.

## Operations

### Selection

```sql
SELECT a_10
FROM A
WHERE a_100 < ?;
```

Find all values in `a_10` for which the corresponding `a_100` is less than some specified value.

Parameters:

- Selectivity: [1, 5, 25].
- Output format: [hitmap, indices, packed values].

### Semijoin

```sql
SELECT a_10
FROM A, B
WHERE a_b_k = b_k
  AND b_100 < ?;
```

Find all values in `a_10` for which the corresponding `b_100` is less than some specified value. To isolate the cost of the semijoin, exclude the cost of finding `b_100 < ?`.

Parameters:

- Selectivity: [1, 5, 25].
- Output format: [hitmap, indices, packed values]

### Join

```sql
SELECT a_10, b_10
FROM A, B
WHERE a_b_k = b_k
  AND b_100 < ?;
```

Find all values in `a_10` and the corresponding `b_10` for which the corresponding `b_100` is less than some specified value. To isolate the cost of the join, exclude the cost of finding `b_100 < ?`.

Parameters:

- Selectivity: [1, 5, 25].

### Aggregate

```sql
SELECT SUM(a_10)
FROM A;
```

Compute the sum of the values in `a_10`.

### Group-by aggregate

```sql
SELECT a_100, SUM(a_10)
FROM A
GROUP BY a_100;
```

For each distinct value of `a_100`, compute the sum of the corresponding values in `a_10`. 
