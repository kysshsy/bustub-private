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

  this->directory_page_id_ = dir_page_id;
  buffer_pool_manager_->UnpinPage(bucket_page_id, false);
  buffer_pool_manager_->UnpinPage(dir_page_id, true);
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
  auto dir_page = reinterpret_cast<HashTableDirectoryPage *>(FetchDirectoryPage());
  uint32_t bucket_idx = KeyToDirectoryIndex(key, dir_page);
  HASH_TABLE_BUCKET_TYPE *bucket_page = FetchBucketPage(dir_page->GetBucketPageId(bucket_idx));

  bool res = bucket_page->GetValue(key, comparator_, result);
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
  auto dir_page = reinterpret_cast<HashTableDirectoryPage *>(FetchDirectoryPage());
  uint32_t bucket_idx = KeyToDirectoryIndex(key, dir_page);
  page_id_t bucket_page_id = dir_page->GetBucketPageId(bucket_idx);
  HASH_TABLE_BUCKET_TYPE *bucket_page = FetchBucketPage(bucket_page_id);
  if (bucket_page->IsFull()) {
    // 释放
    buffer_pool_manager_->UnpinPage(directory_page_id_, false);
    buffer_pool_manager_->UnpinPage(bucket_page_id, false);
    return SplitInsert(transaction, key, value);
  }

  bool result = bucket_page->Insert(key, value, comparator_);
  buffer_pool_manager_->UnpinPage(directory_page_id_, false);
  buffer_pool_manager_->UnpinPage(bucket_page_id, result);
  return result;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::SplitInsert(Transaction *transaction, const KeyType &key, const ValueType &value) {
  auto dir_page = reinterpret_cast<HashTableDirectoryPage *>(FetchDirectoryPage());
  uint32_t bucket_idx = KeyToDirectoryIndex(key, dir_page);
  uint32_t gd = dir_page->GetGlobalDepth();
  uint32_t ld = dir_page->GetLocalDepth(bucket_idx);
  
  page_id_t bucket_page_id = dir_page->GetBucketPageId(bucket_idx);
  page_id_t split_page_id;
  Page *tmp_page = buffer_pool_manager_->NewPage(&split_page_id);
  if (tmp_page == nullptr) {
    buffer_pool_manager_->UnpinPage(directory_page_id_, false);
    return false;
  }
  HASH_TABLE_BUCKET_TYPE *split_page = reinterpret_cast<HASH_TABLE_BUCKET_TYPE*>(tmp_page->GetData());
  HASH_TABLE_BUCKET_TYPE *bucket_page = reinterpret_cast<HASH_TABLE_BUCKET_TYPE*>(FetchBucketPage(bucket_page_id));
  /*
   * 1. local depth小于 global depth，则只需要分裂一倍 bucket 并且增加local depth
   * 2. local depth等于 global depth, directory page的槽需要增加 增加新增部分槽的映射关系
   */
  if (gd == ld) {
    dir_page->GrowDirectory();
    dir_page->IncrGlobalDepth();
  }
  // 分裂是肯定需要的
  uint32_t size = dir_page->Size();
  for (uint32_t i = 0; i < size; i++) {
    page_id_t cur_page_id = dir_page->GetBucketPageId(i);
    if (cur_page_id == bucket_page_id) {
      dir_page->IncrLocalDepth(i);
      auto target_idx = dir_page->GetSplitImageIndex(i);
      assert(dir_page->GetBucketPageId(target_idx) == bucket_page_id);
      assert(dir_page->GetLocalDepth(target_idx) == ld);
      dir_page->SetBucketPageId(target_idx, split_page_id);
      dir_page->IncrLocalDepth(target_idx);
    }
  }
  

  uint32_t split_taken = 0;
  uint32_t bucket_taken = 0;
  // 收集原页的kv 重新插入 ? (使用KeyToDirectoryIndex)
  for (uint32_t i = 0; i < BUCKET_ARRAY_SIZE; i++) {
    auto k = bucket_page->KeyAt(i);
    page_id_t target_page_id = dir_page->GetBucketPageId(KeyToDirectoryIndex(k, dir_page));
    assert(target_page_id == bucket_page_id || target_page_id == split_page_id);
    if (target_page_id == split_page_id) {
      split_taken++;
      bucket_taken++;
      split_page->Insert(k, bucket_page->ValueAt(i), comparator_);
      bucket_page->RemoveAt(i);
    }
  }
  // 新增key value
  assert(dir_page->GetBucketPageId(KeyToDirectoryIndex(key, dir_page)) == bucket_page_id ||
         dir_page->GetBucketPageId(KeyToDirectoryIndex(key, dir_page)) == split_page_id);
  if (dir_page->GetBucketPageId(KeyToDirectoryIndex(key, dir_page)) == bucket_page_id) {
    bucket_taken++;
    bucket_page->Insert(key, value, comparator_);
  } else {
    split_taken++;
    split_page->Insert(key, value, comparator_);
  }

  buffer_pool_manager_->UnpinPage(directory_page_id_, true);
  buffer_pool_manager_->UnpinPage(bucket_page_id, bucket_taken != 0);
  buffer_pool_manager_->UnpinPage(split_page_id, split_taken != 0);
  return true;
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::Remove(Transaction *transaction, const KeyType &key, const ValueType &value) {
  auto dir_page = reinterpret_cast<HashTableDirectoryPage *>(FetchDirectoryPage());
  uint32_t bucket_idx = KeyToDirectoryIndex(key, dir_page);
  page_id_t bucket_page_id = dir_page->GetBucketPageId(bucket_idx);
  HASH_TABLE_BUCKET_TYPE *bucket_page = FetchBucketPage(bucket_page_id);
  if (bucket_page->Remove(key, value, comparator_) == false) {
    buffer_pool_manager_->UnpinPage(directory_page_id_, false);
    buffer_pool_manager_->UnpinPage(bucket_page_id, false);
    return false;
  }
  if (bucket_page->IsEmpty()) {
    Merge(transaction, key, value);
  }

  buffer_pool_manager_->UnpinPage(directory_page_id_, false);
  buffer_pool_manager_->UnpinPage(bucket_page_id, true);
  return true;
}

/*****************************************************************************
 * MERGE
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_TYPE::Merge(Transaction *transaction, const KeyType &key, const ValueType &value) {
  /*
    To keep things relatively simple, we provide the following rules for merging:

    Only empty buckets can be merged.
    Buckets can only be merged with their split image if their split image has the same local depth.
    Buckets can only be merged if their local depth is greater than 0.
  */
  auto dir_page = reinterpret_cast<HashTableDirectoryPage *>(FetchDirectoryPage());
  uint32_t bucket_idx = KeyToDirectoryIndex(key, dir_page);
  uint32_t split_idx = dir_page->GetSplitImageIndex(bucket_idx);
  page_id_t bucket_page_id = dir_page->GetBucketPageId(bucket_idx);
  page_id_t split_page_id = dir_page->GetBucketPageId(split_idx);
  uint32_t bucket_depth = dir_page->GetLocalDepth(bucket_idx);
  uint32_t split_depth = dir_page->GetLocalDepth(split_idx);
    
  if (bucket_depth == 0 || bucket_page_id == split_page_id || bucket_depth != split_depth) {
    buffer_pool_manager_->UnpinPage(directory_page_id_, false);
    return;
  }
  HASH_TABLE_BUCKET_TYPE * bucket_page = reinterpret_cast<HASH_TABLE_BUCKET_TYPE *>(FetchBucketPage(bucket_page_id));
  // 并发问题 可能在remove到merge之间 发生了改变
  if (!bucket_page->IsEmpty()) {
    buffer_pool_manager_->UnpinPage(directory_page_id_, false);
    buffer_pool_manager_->UnpinPage(bucket_page_id, false);
    return;
  }

  // 建立映射 减少local depth
  for (uint32_t i = 0; i < dir_page->Size(); i++) {
    page_id_t cur_page_id = dir_page->GetBucketPageId(i);
    if ( cur_page_id == bucket_page_id) {
      dir_page->SetBucketPageId(i, split_page_id);
      dir_page->SetLocalDepth(i, split_depth - 1);
    } else if (cur_page_id == split_page_id) {
      dir_page->DecrLocalDepth(i);
    }
  }

  while (dir_page->CanShrink()) {
    // 直接减少 slot大小
    dir_page->DecrGlobalDepth();
  }  

  buffer_pool_manager_->UnpinPage(directory_page_id_, true);
  buffer_pool_manager_->UnpinPage(bucket_page_id, false);
  buffer_pool_manager_->DeletePage(bucket_page_id);
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
