//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_replacer.cpp
//
// Identification: src/buffer/lru_replacer.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_replacer.h"
#include <algorithm>

namespace bustub {

LRUReplacer::LRUReplacer(size_t num_pages) : max_size_(num_pages) {}

LRUReplacer::~LRUReplacer() = default;

bool LRUReplacer::Victim(frame_id_t *frame_id) {
  std::scoped_lock lock(latch_);
  if (set_.empty()) {
    return false;
  }

  frame_id_t victim = *(this->list_.begin());
  this->list_.pop_front();
  this->set_.erase(victim);

  if (frame_id != nullptr) {
    *frame_id = victim;
  }
  return true;
}

void LRUReplacer::Pin(frame_id_t frame_id) {
  std::scoped_lock lock(latch_);
  if (set_.find(frame_id) == set_.end()) {
    return;
  }

  list_.erase(set_[frame_id]);
  set_.erase(frame_id);
}

void LRUReplacer::Unpin(frame_id_t frame_id) {
  std::scoped_lock lock(latch_);
  if (set_.find(frame_id) != set_.end() || set_.size() >= max_size_) {
    return;
  }
  list_.push_back(frame_id);
  set_.insert({frame_id, --list_.end()});
}

size_t LRUReplacer::Size() {
  std::scoped_lock lock(latch_);
  return this->set_.size();
}

}  // namespace bustub
