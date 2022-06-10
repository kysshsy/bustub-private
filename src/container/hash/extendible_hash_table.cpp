//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// extendible_hash_table.cpp
//
// Identification: src/container/hash/extendible_hash_table.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <iostream>
#include <string>
#include <utility>
#include <vector>

#include "common/exception.h"
#include "common/logger.h"
#include "common/rid.h"
#include "container/hash/extendible_hash_table.h"

namespace bustub {

template <typename KeyType, typename ValueType, typename KeyComparator>
HASH_TABLE_TYPE::ExtendibleHashTable(const std::string &name, BufferPoolManager *buffer_pool_manager,
                                     const KeyComparator &comparator, HashFunction<KeyType> hash_fn)
    : buffer_pool_manager_(buffer_pool_manager), comparator_(comparator), hash_fn_(std::move(hash_fn)) {
  //  implement me!
  page_id_t dir_page_id;
  auto dir_page = reinterpret_cast<HashTableDirectoryPage *>(buffer_pool_manager_->NewPage(&dir_page_id));
  // 初始化 dir_page
  page_id_t bucket_page_id;
  buffer_pool_manager_->NewPage(&bucket_page_id);
  dir_page->SetLocalDepth(0, 0);
  dir_page->SetBucketPageId(0, bucket_page_id);

  buffer_pool_manager_->UnpinPage(bucket_page_id, false);

  // 设置hash_table -> dir_page
  this->directory_page_id_ = dir_page_id;
}

/*****************************************************************************
 * HELPERS
 *****************************************************************************/
/**
 * Hash - simple helper to downcast MurmurHash's 64-bit hash to 32-bit
 * for extendible hashing.
 *
 * @param key the key to hash
 * @return the downcasted 32-bit hash
 */
template <typename KeyType, typename ValueType, typename KeyComparator>
uint32_t HASH_TABLE_TYPE::Hash(KeyType key) {
  return static_cast<uint32_t>(hash_fn_.GetHash(key));
}

template <typename KeyType, typename ValueType, typename KeyComparator>
inline uint32_t HASH_TABLE_TYPE::KeyToDirectoryIndex(KeyType key, HashTableDirectoryPage *dir_page) {
  return Hash(key) & dir_page->GetGlobalDepthMask();
}

template <typename KeyType, typename ValueType, typename KeyComparator>
inline uint32_t HASH_TABLE_TYPE::KeyToPageId(KeyType key, HashTableDirectoryPage *dir_page) {
  return dir_page->GetBucketPageId(KeyToDirectoryIndex(key, dir_page));
}

template <typename KeyType, typename ValueType, typename KeyComparator>
HashTableDirectoryPage *HASH_TABLE_TYPE::FetchDirectoryPage() {
  return reinterpret_cast<HashTableDirectoryPage *>(buffer_pool_manager_->FetchPage(directory_page_id_)->GetData());
}

template <typename KeyType, typename ValueType, typename KeyComparator>
HASH_TABLE_BUCKET_TYPE *HASH_TABLE_TYPE::FetchBucketPage(page_id_t bucket_page_id) {
  return reinterpret_cast<HASH_TABLE_BUCKET_TYPE *>(buffer_pool_manager_->FetchPage(bucket_page_id)->GetData());
}

/*****************************************************************************
 * SEARCH
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::GetValue(Transaction *transaction, const KeyType &key, std::vector<ValueType> *result) {
  // 在bucket page实现过类似功能 只需要 找好映射 调用即可

  // 每次都对页执行操作都fetch一遍 然后释放 unpin
  table_latch_.RLock();
  auto dir_page = reinterpret_cast<HashTableDirectoryPage *>(FetchDirectoryPage());
  uint32_t bucket_idx = KeyToDirectoryIndex(key, dir_page);
  HASH_TABLE_BUCKET_TYPE *bucket_page = FetchBucketPage(dir_page->GetBucketPageId(bucket_idx));
  table_latch_.RUnlock();

  auto bpage = reinterpret_cast<Page *>(bucket_page);
  bpage->RLatch();
  bool res = bucket_page->GetValue(key, comparator_, result);
  bpage->RUnlatch();

  // 统一释放 ? 提前释放dir 会不会快一点
  buffer_pool_manager_->UnpinPage(dir_page->GetPageId(), false);
  buffer_pool_manager_->UnpinPage(dir_page->GetBucketPageId(bucket_idx), false);
  return res;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::Insert(Transaction *transaction, const KeyType &key, const ValueType &value) {
  table_latch_.RLock();
  auto dir_page = reinterpret_cast<HashTableDirectoryPage *>(FetchDirectoryPage());
  uint32_t bucket_idx = KeyToDirectoryIndex(key, dir_page);
  HASH_TABLE_BUCKET_TYPE *bucket_page = FetchBucketPage(dir_page->GetBucketPageId(bucket_idx));
  auto bpage = reinterpret_cast<Page *>(bucket_page);
  bpage->WLatch();
  if (bucket_page->IsFull()) {
    // 释放
    bpage->WUnlatch();
    table_latch_.RUnlock();
    buffer_pool_manager_->UnpinPage(dir_page->GetPageId(), false);
    buffer_pool_manager_->UnpinPage(dir_page->GetBucketPageId(bucket_idx), false);
    return SplitInsert(transaction, key, value);
  }

  bool result = bucket_page->Insert(key, value, comparator_);
  bpage->WUnlatch();
  table_latch_.RUnlock();
  buffer_pool_manager_->UnpinPage(dir_page->GetPageId(), false);
  buffer_pool_manager_->UnpinPage(dir_page->GetBucketPageId(bucket_idx), result);
  return result;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::SplitInsert(Transaction *transaction, const KeyType &key, const ValueType &value) {
  table_latch_.WLock();
  auto dir_page = reinterpret_cast<HashTableDirectoryPage *>(FetchDirectoryPage());
  uint32_t bucket_idx = KeyToDirectoryIndex(key, dir_page);
  /*
   * 1. local depth小于 global depth，则只需要增加一页 并且增加local depth
   * 2. local depth等于 global depth, directory page的槽需要增加 增加新增部分槽的映射关系
   */
  if (dir_page->GetLocalDepth(bucket_idx) == dir_page->GetGlobalDepth()) {
    for (uint32_t i = 0; i < dir_page->Size(); i++) {
      // 计算对应的新增槽
      uint32_t new_slot = i | (0x1 << (dir_page->GetGlobalDepth()));
      dir_page->SetBucketPageId(new_slot, dir_page->GetBucketPageId(i));
      dir_page->SetLocalDepth(new_slot, dir_page->GetLocalDepth(i));
    }
    dir_page->IncrGlobalDepth();
  }
  // 增加新页 增加local depth (注意是 桶对应的所有槽的local depth)
  auto bucket_page_id = dir_page->GetBucketPageId(bucket_idx);
  auto incr_depth = dir_page->GetLocalDepth(bucket_idx) + 1;
  for (uint32_t i = 0; i < dir_page->Size(); i++) {
    if (dir_page->GetBucketPageId(i) == bucket_page_id) {
      dir_page->SetLocalDepth(i, static_cast<uint8_t>(incr_depth));
    }
  }

  uint32_t split_idx = dir_page->GetSplitImageIndex(bucket_idx);
  page_id_t split_page_id;
  HASH_TABLE_BUCKET_TYPE *split_page =
      reinterpret_cast<HASH_TABLE_BUCKET_TYPE *>(buffer_pool_manager_->NewPage(&split_page_id)->GetData());
  if (split_page == nullptr) {
    table_latch_.WUnlock();
    buffer_pool_manager_->UnpinPage(dir_page->GetPageId(), true);
    return false;
  }
  Page *spage = reinterpret_cast<Page *>(split_page);
  spage->WLatch();
  dir_page->SetBucketPageId(split_idx, split_page_id);

  HASH_TABLE_BUCKET_TYPE *bucket_page = FetchBucketPage(dir_page->GetBucketPageId(bucket_idx));
  Page *bpage = reinterpret_cast<Page *>(bucket_page);
  bpage->WLatch();

  uint32_t local_depth = dir_page->GetLocalDepth(bucket_idx);
  dir_page->SetLocalDepth(split_idx, static_cast<uint8_t>(local_depth));

  uint32_t bucket_high = dir_page->GetLocalHighBit(bucket_idx);
  uint32_t local_mask = dir_page->GetLocalDepthMask(bucket_idx);
  // range 原来的页 重新分配元素 映射到 原页和新页
  uint32_t bucket_taken = 0;
  uint32_t split_taken = 0;
  for (uint32_t i = 0; i < BUCKET_ARRAY_SIZE; i++) {
    if (!bucket_page->IsOccupied(i)) {
      break;
    }

    if (!bucket_page->IsReadable(i)) {
      continue;
    }
    if ((Hash(bucket_page->KeyAt(i)) & local_mask) != bucket_high) {
      // 转移
      bucket_taken++;
      split_taken++;
      split_page->Insert(bucket_page->KeyAt(i), bucket_page->ValueAt(i), comparator_);
      bucket_page->RemoveAt(i);
    }
  }
  // 还有一个新增的
  if ((Hash(key) & local_mask) == bucket_high) {
    bucket_taken++;
    bucket_page->Insert(key, value, comparator_);
  } else {  // Q: 如果 还是插入到了bucket_page怎么办 会不会出现空间不够的情况?
    split_taken++;
    split_page->Insert(key, value, comparator_);
  }

  bpage->WUnlatch();
  spage->WUnlatch();
  table_latch_.WUnlock();
  buffer_pool_manager_->UnpinPage(dir_page->GetPageId(), true);
  buffer_pool_manager_->UnpinPage(dir_page->GetBucketPageId(bucket_idx), bucket_taken != 0);
  buffer_pool_manager_->UnpinPage(split_page_id, split_taken != 0);

  return true;
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::Remove(Transaction *transaction, const KeyType &key, const ValueType &value) {
  table_latch_.RLock();
  auto dir_page = reinterpret_cast<HashTableDirectoryPage *>(FetchDirectoryPage());
  uint32_t bucket_idx = KeyToDirectoryIndex(key, dir_page);

  HASH_TABLE_BUCKET_TYPE *bucket_page = FetchBucketPage(dir_page->GetBucketPageId(bucket_idx));
  Page *bpage = reinterpret_cast<Page *>(bucket_page);
  bpage->WLatch();
  bool result = bucket_page->Remove(key, value, comparator_);
  bool empty = bucket_page->IsEmpty();

  bpage->WUnlatch();
  table_latch_.RUnlock();
  buffer_pool_manager_->UnpinPage(dir_page->GetPageId(), false);
  buffer_pool_manager_->UnpinPage(dir_page->GetBucketPageId(bucket_idx), result);
  if (!result) {
    return false;
  }
  if (empty) {
    Merge(transaction, key, value);
  }
  return true;
}

/*****************************************************************************
 * MERGE
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_TYPE::Merge(Transaction *transaction, const KeyType &key, const ValueType &value) {
  table_latch_.WLock();
  auto dir_page = reinterpret_cast<HashTableDirectoryPage *>(FetchDirectoryPage());
  uint32_t bucket_idx = KeyToDirectoryIndex(key, dir_page);
  /*
    To keep things relatively simple, we provide the following rules for merging:

    Only empty buckets can be merged.
    Buckets can only be merged with their split image if their split image has the same local depth.
    Buckets can only be merged if their local depth is greater than 0.
  */
  uint32_t split_idx = dir_page->GetSplitImageIndex(bucket_idx);
  page_id_t bucket_page_id = dir_page->GetBucketPageId(bucket_idx);
  page_id_t split_page_id = dir_page->GetBucketPageId(split_idx);
  if (dir_page->GetLocalDepth(bucket_idx) == 0 || dir_page->GetLocalDepth(split_idx) == 0 ||
      bucket_page_id == split_page_id) {
    table_latch_.WUnlock();
    return;
  }
  if (dir_page->GetLocalDepth(bucket_idx) != dir_page->GetLocalDepth(split_idx)) {
    table_latch_.WUnlock();
    return;
  }
  for (uint32_t i = 0; i < dir_page->Size(); i++) {
    if (dir_page->GetBucketPageId(i) == bucket_page_id) {
      dir_page->SetBucketPageId(i, split_page_id);
    }
    if (dir_page->GetBucketPageId(i) == split_page_id) {
      dir_page->DecrLocalDepth(i);
    }
  }
  table_latch_.WUnlock();
  buffer_pool_manager_->DeletePage(dir_page->GetBucketPageId(bucket_idx));
}

/*****************************************************************************
 * GETGLOBALDEPTH - DO NOT TOUCH
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
uint32_t HASH_TABLE_TYPE::GetGlobalDepth() {
  table_latch_.RLock();
  HashTableDirectoryPage *dir_page = FetchDirectoryPage();
  uint32_t global_depth = dir_page->GetGlobalDepth();
  assert(buffer_pool_manager_->UnpinPage(directory_page_id_, false, nullptr));
  table_latch_.RUnlock();
  return global_depth;
}

/*****************************************************************************
 * VERIFY INTEGRITY - DO NOT TOUCH
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_TYPE::VerifyIntegrity() {
  table_latch_.RLock();
  HashTableDirectoryPage *dir_page = FetchDirectoryPage();
  dir_page->VerifyIntegrity();
  assert(buffer_pool_manager_->UnpinPage(directory_page_id_, false, nullptr));
  table_latch_.RUnlock();
}

/*****************************************************************************
 * TEMPLATE DEFINITIONS - DO NOT TOUCH
 *****************************************************************************/
template class ExtendibleHashTable<int, int, IntComparator>;

template class ExtendibleHashTable<GenericKey<4>, RID, GenericComparator<4>>;
template class ExtendibleHashTable<GenericKey<8>, RID, GenericComparator<8>>;
template class ExtendibleHashTable<GenericKey<16>, RID, GenericComparator<16>>;
template class ExtendibleHashTable<GenericKey<32>, RID, GenericComparator<32>>;
template class ExtendibleHashTable<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
