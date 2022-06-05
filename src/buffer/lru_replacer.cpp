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

LRUReplacer::LRUReplacer(size_t num_pages) : max_size(num_pages) {}

LRUReplacer::~LRUReplacer() = default;

bool LRUReplacer::Victim(frame_id_t *frame_id) {
  std::scoped_lock lock(latch_);
  if (set.empty()) {
    return false;
  }

  frame_id_t victim = *(this->list.begin());
  this->list.pop_front();
  this->set.erase(victim);

  if (frame_id != nullptr) {
    *frame_id = victim;
  }
  return true;
}

void LRUReplacer::Pin(frame_id_t frame_id) {
  std::scoped_lock lock(latch_);
  if (set.find(frame_id) == set.end()) {
    return;
  }

  list.erase(set[frame_id]);
  set.erase(frame_id);
}

void LRUReplacer::Unpin(frame_id_t frame_id) {
  std::scoped_lock lock(latch_);
  if (set.find(frame_id) != set.end() || set.size() >= max_size) {
    return;
  }
  list.push_back(frame_id);
  set.insert({frame_id, --list.end()});
}

size_t LRUReplacer::Size() {
  std::scoped_lock lock(latch_);
  return this->set.size();
}

}  // namespace bustub
