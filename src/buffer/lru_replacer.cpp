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

namespace bustub {

LRUReplacer::LRUReplacer(size_t num_pages) { maxLen = num_pages; }

LRUReplacer::~LRUReplacer() = default;

bool LRUReplacer::Victim(frame_id_t *frame_id) {
  lock.lock();
  if (list_frame_id.empty()) {
    lock.unlock();
    return false;
  }
  frame_id_t pop_front_frame_id = list_frame_id.back();
  map_frame_id.erase(pop_front_frame_id);
  list_frame_id.pop_back();
  *frame_id = pop_front_frame_id;
  lock.unlock();
  return true;
}

void LRUReplacer::Pin(frame_id_t frame_id) {
  lock.lock();
  if (map_frame_id.count(frame_id) != 0) {
    list_frame_id.erase(map_frame_id[frame_id]);
    map_frame_id.erase(frame_id);
  }
  lock.unlock();
}

void LRUReplacer::Unpin(frame_id_t frame_id) {
  lock.lock();
  if (map_frame_id.count(frame_id) != 0) {
    lock.unlock();
    return;
  }
  for (; Size() >= maxLen;) {
    frame_id_t tmp_frame_id = list_frame_id.front();
    list_frame_id.pop_front();
    map_frame_id.erase(tmp_frame_id);
  }
  list_frame_id.push_front(frame_id);
  map_frame_id[frame_id] = list_frame_id.begin();
  lock.unlock();
}

size_t LRUReplacer::Size() { return list_frame_id.size(); }

}  // namespace bustub
