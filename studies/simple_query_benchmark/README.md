# Simple Query Benchmark (SQB)

## Schema

### A

| Column | Domain                                  |
| ------ | --------------------------------------- |
| a_k    | $\left[ 0, \lvert A \rvert - 1 \right]$ |
| a_b_k  | $\left[ 0, \lvert B \rvert - 1 \right]$ |
| a_10   | $\left[ 0, 9 \right]$                   |
| a_100  | $\left[ 0, 99 \right]$                  |

### B

| Column | Domain                                  |
| ------ | --------------------------------------- |
| b_k    | $\left[ 0, \lvert B \rvert - 1 \right]$ |
| b_10   | $\left[ 0, 9 \right]$                   |
| b_100  | $\left[ 0, 99 \right]$                  |

## Queries

### SQ1

```sql
SELECT SUM(a_10)
FROM A;
```

### SQ2

```sql
SELECT SUM(a_10)
FROM A
WHERE a_100 < ?;
```

### SQ3

```sql
SELECT SUM(a_10)
FROM A, B
WHERE a_b_k = b_k
  AND b_100 < ?;
```

### SQ4

```sql
SELECT b_10, SUM(a_10)
FROM A, B
WHERE a_b_k = b_k
  AND b_100 < ?
GROUP BY b_10
ORDER BY b_10;
```
