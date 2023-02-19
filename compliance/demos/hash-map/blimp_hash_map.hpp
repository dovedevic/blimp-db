#ifndef BLIMP_HASH_BLIMP_HASH_MAP_HPP
#define BLIMP_HASH_BLIMP_HASH_MAP_HPP

#include <array>
#include <cstddef>
#include <cstdint>
#include <utility>
#include <vector>

class BlimpHashMap {
public:
  /**
   * Construct a BlimpHashMap.
   * @param capacity Determines how many bucket chains will be created. It is
   *    possible to exceed this capacity through the insertion of key-value
   *    pairs. However, the smaller the capacity relative to the number of
   *    key-value pairs, the longer the expected bucket chains.
   */
  explicit BlimpHashMap(size_t capacity) {
    // Compute the number of buckets needed to satisfy the capacity requirement.
    size_t k = capacity / bucket_capacity_ + (capacity % bucket_capacity_ != 0);

    // Increase the number of buckets to the next highest power of two.
    size_t num_buckets = 1;
    while (num_buckets < k) {
      num_buckets <<= 1;
    }

    buckets_ = std::vector<Bucket>(num_buckets);
    mask_ = num_buckets - 1;
  }

  /**
   * Insert a key-value pair.
   * @param key
   * @param value
   */
  void insert(uint32_t key, uint32_t value) {
    // Find the initial bucket in the chain.
    Bucket &bucket = buckets_[hash(key)];

    // Advance to the end of the chain.
    while (*bucket.next() != UINT32_MAX) {
      bucket = buckets_[*bucket.next()];
    }

    // If the bucket is full, attempt to add another bucket to the chain.
    if (*bucket.count() == bucket_capacity_) {
      if (buckets_.size() == UINT32_MAX) {
        throw std::runtime_error("capacity exceeded");
      }

      *bucket.next() = buckets_.size();
      buckets_.emplace_back();
      bucket = buckets_.back();
    }

    // Write the item to the left-most slot in the bucket.
    uint32_t *item = bucket.item(*bucket.count());
    item[0] = key;
    item[1] = value;

    ++*bucket.count();
  }

  /**
   * Get the value for a key.
   * @param key
   * @return The value for the key, or NULL if the key is not in the map.
   */
  uint32_t *get(uint32_t key) {
    uint32_t bucket_index = hash(key);
    do {
      // Find the initial bucket in the chain.
      Bucket &bucket = buckets_[bucket_index];

      // If the item is in the bucket, return the item.
      for (size_t i = 0; i < bucket_capacity_; ++i) {
        uint32_t *item = bucket.item(i);
        if (item[0] == key) {
          return &item[1];
        }
      }

      // Advance to the next bucket in the chain.
      bucket_index = *bucket.next();
    } while (bucket_index != UINT32_MAX);

    // The end of the chain was reached and the item was not found.
    return nullptr;
  }

private:
  static constexpr size_t bucket_size_ = 128;
  static constexpr size_t bucket_capacity_ = (bucket_size_ - 8) / 8;

  struct Bucket {
    std::array<uint8_t, bucket_size_> data_;

    Bucket() : data_() {
      *count() = 0;
      *next() = UINT32_MAX;
    }

    uint32_t *item(size_t i) { return (uint32_t *)&data_[i * 8]; }

    uint32_t *count() { return (uint32_t *)&data_[bucket_size_ - 8]; }

    uint32_t *next() { return (uint32_t *)&data_[bucket_size_ - 4]; }
  };

  std::vector<Bucket> buckets_;
  uint32_t mask_;

  [[nodiscard]] uint32_t hash(uint32_t key) const {
    return (3634946921 * key + 2096170329) & mask_;
  }
};

#endif // BLIMP_HASH_BLIMP_HASH_MAP_HPP
