//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// parallel_buffer_pool_manager.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/parallel_buffer_pool_manager.h"

namespace bustub {

ParallelBufferPoolManager::ParallelBufferPoolManager(size_t num_instances, size_t pool_size, DiskManager *disk_manager,
                                                     LogManager *log_manager)
    : pool_size_(pool_size), num_instances_(num_instances) {
  // Allocate and create individual BufferPoolManagerInstances
  for (uint32_t i = 0; i < num_instances; i++) {
    managers_.push_back(
        new BufferPoolManagerInstance{pool_size, static_cast<uint32_t>(num_instances), i, disk_manager, log_manager});
  }
}

// Update constructor to destruct all BufferPoolManagerInstances and deallocate any associated memory
ParallelBufferPoolManager::~ParallelBufferPoolManager() = default;

size_t ParallelBufferPoolManager::GetPoolSize() {
  // Get size of all BufferPoolManagerInstances
  return num_instances_ * pool_size_;
}

BufferPoolManager *ParallelBufferPoolManager::GetBufferPoolManager(page_id_t page_id) {
  // Get BufferPoolManager responsible for handling given page id. You can use this method in your other methods.
  return managers_[page_id % num_instances_];
}

Page *ParallelBufferPoolManager::FetchPgImp(page_id_t page_id) {
  // Fetch page for page_id from responsible BufferPoolManagerInstance
  auto &buffer_pool = managers_[page_id % num_instances_];
  return buffer_pool->FetchPage(page_id);
}

bool ParallelBufferPoolManager::UnpinPgImp(page_id_t page_id, bool is_dirty) {
  // Unpin page_id from responsible BufferPoolManagerInstance
  auto &buffer_pool = managers_[page_id % num_instances_];
  return buffer_pool->UnpinPage(page_id, is_dirty);
}

bool ParallelBufferPoolManager::FlushPgImp(page_id_t page_id) {
  // Flush page_id from responsible BufferPoolManagerInstance
  auto &buffer_pool = managers_[page_id % num_instances_];
  return buffer_pool->FlushPage(page_id);
}

Page *ParallelBufferPoolManager::NewPgImp(page_id_t *page_id) {
  // create new page. We will request page allocation in a round robin manner from the underlying
  // BufferPoolManagerInstances
  // 1.   From a starting index of the BPMIs, call NewPageImpl until either 1) success and return 2) looped around to
  // starting index and return nullptr
  // 2.   Bump the starting index (mod number of instances) to start search at a different BPMI each time this function
  // is called
  size_t start_index = cur_instance_ % num_instances_;
  cur_instance_ = (cur_instance_ + 1) % num_instances_;
  size_t index = start_index;
  do {
    auto &buffer_pool = managers_[index];

    auto page = buffer_pool->NewPage(page_id);
    if (page != nullptr) {
      return page;
    }

    index = (index + 1) % num_instances_;
  } while (index != start_index);

  return nullptr;
}

bool ParallelBufferPoolManager::DeletePgImp(page_id_t page_id) {
  // Delete page_id from responsible BufferPoolManagerInstance
  auto &buffer_pool = managers_[page_id % num_instances_];
  return buffer_pool->DeletePage(page_id);
}

void ParallelBufferPoolManager::FlushAllPgsImp() {
  // flush all pages from all BufferPoolManagerInstances

  for (auto &bp : managers_) {
    bp->FlushAllPages();
  }
}

}  // namespace bustub
