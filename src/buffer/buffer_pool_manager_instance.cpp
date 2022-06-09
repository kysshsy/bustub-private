//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager_instance.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager_instance.h"

#include "common/macros.h"

namespace bustub {

BufferPoolManagerInstance::BufferPoolManagerInstance(size_t pool_size, DiskManager *disk_manager,
                                                     LogManager *log_manager)
    : BufferPoolManagerInstance(pool_size, 1, 0, disk_manager, log_manager) {}

BufferPoolManagerInstance::BufferPoolManagerInstance(size_t pool_size, uint32_t num_instances, uint32_t instance_index,
                                                     DiskManager *disk_manager, LogManager *log_manager)
    : pool_size_(pool_size),
      num_instances_(num_instances),
      instance_index_(instance_index),
      next_page_id_(instance_index),
      disk_manager_(disk_manager),
      log_manager_(log_manager) {
  BUSTUB_ASSERT(num_instances > 0, "If BPI is not part of a pool, then the pool size should just be 1");
  BUSTUB_ASSERT(
      instance_index < num_instances,
      "BPI index cannot be greater than the number of BPIs in the pool. In non-parallel case, index should just be 1.");
  // We allocate a consecutive memory space for the buffer pool.
  pages_ = new Page[pool_size_];
  replacer_ = new LRUReplacer(pool_size);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }
}

BufferPoolManagerInstance::~BufferPoolManagerInstance() {
  delete[] pages_;
  delete replacer_;
}

bool BufferPoolManagerInstance::FlushPgImp(page_id_t page_id) {
  // Make sure you call DiskManager::WritePage!
  std::scoped_lock lock(this->latch_);
  assert(page_id != INVALID_PAGE_ID);

  if (page_table_.find(page_id) == page_table_.end()) {
    return false;
  }

  frame_id_t fid = page_table_[page_id];
  Page &pg = pages_[fid];

  assert(pg.GetPageId() == page_id);

  if (pg.IsDirty()) {
    disk_manager_->WritePage(page_id, pg.GetData());
    pg.is_dirty_ = false;
  }

  return true;
}

void BufferPoolManagerInstance::FlushAllPgsImp() {
  // You can do it!
  std::scoped_lock lock(this->latch_);

  for (auto kv : page_table_) {
    assert(kv.first >= 0 && (size_t)kv.first < this->pool_size_);

    auto &pg = pages_[kv.first];
    if (pg.IsDirty()) {
      disk_manager_->WritePage(pg.GetPageId(), pg.GetData());
      pg.is_dirty_ = false;
    }
  }
}

Page *BufferPoolManagerInstance::NewPgImp(page_id_t *page_id) {
  // 0.   Make sure you call AllocatePage!
  // 1.   If all the pages in the buffer pool are pinned, return nullptr.
  // 2.   Pick a victim page P from either the free list or the replacer. Always pick from the free list first.
  // 3.   Update P's metadata, zero out memory and add P to the page table.
  // 4.   Set the page ID output parameter. Return a pointer to P.
  std::scoped_lock lock(this->latch_);

  frame_id_t fid;
  if (!PickVictim(&fid)) {
    return nullptr;
  }

  Page &pg = pages_[fid];

  if (pg.GetPageId() != INVALID_PAGE_ID) {
    page_table_.erase(pg.GetPageId());
  }
  if (pg.GetPageId() != INVALID_PAGE_ID && pg.IsDirty()) {
    disk_manager_->WritePage(pg.GetPageId(), pg.GetData());
  }

  auto pid = AllocatePage();
  page_table_[pid] = fid;

  // 初始化page
  pg.ResetMemory();
  pg.pin_count_ = 1;
  pg.page_id_ = pid;
  pg.is_dirty_ = false;

  if (page_id != nullptr) {
    *page_id = pid;
  }
  return &pg;
}

Page *BufferPoolManagerInstance::FetchPgImp(page_id_t page_id) {
  // 1.     Search the page table for the requested page (P).
  // 1.1    If P exists, pin it and return it immediately.
  // 1.2    If P does not exist, find a replacement page (R) from either the free list or the replacer.
  //        Note that pages are always found from the free list first.
  // 2.     If R is dirty, write it back to the disk.
  // 3.     Delete R from the page table and insert P.
  // 4.     Update P's metadata, read in the page content from disk, and then return a pointer to P.
  std::scoped_lock lock(this->latch_);

  if (page_table_.find(page_id) != page_table_.end()) {
    Page &pg = pages_[page_table_[page_id]];
    pg.pin_count_++;
    replacer_->Pin(page_table_[page_id]);
    return &pg;
  }

  frame_id_t fid;
  if (!PickVictim(&fid)) {
    return nullptr;
  }

  Page &pg = pages_[fid];

  if (pg.GetPageId() != INVALID_PAGE_ID) {
    page_table_.erase(pg.GetPageId());
  }
  if (pg.GetPageId() != INVALID_PAGE_ID && pg.IsDirty()) {
    disk_manager_->WritePage(pg.GetPageId(), pg.GetData());
  }

  // 初始化page
  disk_manager_->ReadPage(page_id, pg.GetData());
  pg.pin_count_ = 1;
  pg.page_id_ = page_id;
  pg.is_dirty_ = false;

  page_table_[page_id] = fid;

  return &pg;
}

bool BufferPoolManagerInstance::DeletePgImp(page_id_t page_id) {
  // 0.   Make sure you call DeallocatePage!
  // 1.   Search the page table for the requested page (P).
  // 1.   If P does not exist, return true.
  // 2.   If P exists, but has a non-zero pin-count, return false. Someone is using the page.
  // 3.   Otherwise, P can be deleted. Remove P from the page table, reset its metadata and return it to the free list.
  std::scoped_lock lock(this->latch_);
  DeallocatePage(page_id);

  if (page_table_.find(page_id) == page_table_.end()) {
    return true;
  }

  frame_id_t fid = page_table_[page_id];
  Page &pg = pages_[fid];
  if (pg.GetPinCount() != 0) {
    return false;
  }

  if (pg.IsDirty()) {
    disk_manager_->WritePage(page_id, pg.GetData());
  }

  pg.ResetMemory();
  pg.page_id_ = INVALID_PAGE_ID;
  pg.is_dirty_ = false;
  pg.pin_count_ = 0;

  // 设置 page_table 和 free list
  page_table_.erase(page_id);
  free_list_.push_back(fid);

  replacer_->Pin(fid);

  return true;
}

bool BufferPoolManagerInstance::UnpinPgImp(page_id_t page_id, bool is_dirty) {
  std::scoped_lock lock(this->latch_);

  if (page_table_.find(page_id) == page_table_.end()) {
    return true;
  }
  frame_id_t fid = page_table_[page_id];
  Page &pg = pages_[fid];

  if (pg.GetPinCount() <= 0) {
    return false;
  }

  if (--pg.pin_count_ == 0) {
    replacer_->Unpin(fid);
  }
  pg.is_dirty_ = is_dirty ? true : pg.is_dirty_;
  return true;
}

page_id_t BufferPoolManagerInstance::AllocatePage() {
  const page_id_t next_page_id = next_page_id_;
  next_page_id_ += num_instances_;
  ValidatePageId(next_page_id);
  return next_page_id;
}

void BufferPoolManagerInstance::ValidatePageId(const page_id_t page_id) const {
  assert(page_id % num_instances_ == instance_index_);  // allocated pages mod back to this BPI
}

bool BufferPoolManagerInstance::PickVictim(frame_id_t *fid) {
  if (!free_list_.empty()) {
    if (fid != nullptr) {
      *fid = free_list_.front();
    }
    free_list_.pop_front();
    return true;
  }

  return replacer_->Victim(fid);
}

}  // namespace bustub
