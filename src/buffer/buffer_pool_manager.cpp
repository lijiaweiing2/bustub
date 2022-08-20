//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager.h"

#include <list>
#include <unordered_map>

namespace bustub {

BufferPoolManager::BufferPoolManager(size_t pool_size, DiskManager *disk_manager, LogManager *log_manager)
    : pool_size_(pool_size), disk_manager_(disk_manager), log_manager_(log_manager) {
  // We allocate a consecutive memory space for the buffer pool.
  pages_ = new Page[pool_size_];
  replacer_ = new LRUReplacer(pool_size);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }
}

BufferPoolManager::~BufferPoolManager() {
  delete[] pages_;
  delete replacer_;
}

Page *BufferPoolManager::FetchPageImpl(page_id_t page_id) {
  // 1.     Search the page table for the requested page (P).
  // 1.1    If P exists, pin it and return it immediately.
  // 1.2    If P does not exist, find a replacement page (R) from either the free list or the replacer.
  //        Note that pages are always found from the free list first.
  // 2.     If R is dirty, write it back to the disk.
  // 3.     Delete R from the page table and insert P.
  // 4.     Update P's metadata, read in the page content from disk, and then return a pointer to P.
  latch_.lock();
  if (page_table_.count(page_id) != 0) {
    replacer_->Pin(page_table_[page_id]);
    pages_[page_table_[page_id]].pin_count_++;
    latch_.unlock();
    return &pages_[page_table_[page_id]];
  }
  frame_id_t tmp_frame_id;
  // List of free pages.非空则直接取出一个
  if (!free_list_.empty()) {
    tmp_frame_id = free_list_.front();
    free_list_.pop_front();
  } else {
    if (replacer_->Victim(&tmp_frame_id)) {
      page_id_t tmp_page_id = INVALID_PAGE_ID;
      for (auto i : page_table_) {
        if (i.second == tmp_frame_id) {
          tmp_page_id = i.first;
          break;
        }
      }
      if (tmp_page_id != INVALID_PAGE_ID) {
        Page *replace_page = &pages_[tmp_frame_id];
        if (pages_[tmp_frame_id].IsDirty()) {
          disk_manager_->WritePage(tmp_page_id, pages_[tmp_frame_id].GetData());
          replace_page->pin_count_ = 0;
        }
        page_table_.erase(replace_page->page_id_);
      }
      //
      Page *new_page = &pages_[tmp_frame_id];
      new_page->page_id_ = page_id;
      page_table_[page_id] = tmp_frame_id;
      disk_manager_->ReadPage(page_id, new_page->data_);
      new_page->pin_count_++;
      new_page->is_dirty_ = false;
      replacer_->Pin(tmp_frame_id);
      latch_.unlock();
      return new_page;
    }
  }

  latch_.unlock();
  return nullptr;
}

bool BufferPoolManager::UnpinPageImpl(page_id_t page_id, bool is_dirty) {
  latch_.lock();
  auto iter = page_table_.find(page_id);
  if (iter == page_table_.end()) {
    latch_.unlock();
    return false;
  }

  if (is_dirty) {
    pages_[iter->second].is_dirty_ = true;
  }
  if (pages_[iter->second].GetPinCount() == 0) {
    latch_.unlock();
    return false;
  }
  frame_id_t tmp_frame_id = iter->second;
  pages_[iter->second].pin_count_--;
  if (pages_[tmp_frame_id].GetPinCount() == 0) {
    replacer_->Unpin(tmp_frame_id);
  }
  latch_.unlock();
  return true;
}

bool BufferPoolManager::FlushPageImpl(page_id_t page_id) {
  // Make sure you call DiskManager::WritePage!
  latch_.lock();
  auto iter = page_table_.find(page_id);
  if (iter == page_table_.end() || iter->first == INVALID_PAGE_ID) {
    latch_.unlock();
    return false;
  }
  disk_manager_->WritePage(page_id, pages_[iter->second].GetData());
  latch_.unlock();
  return false;
}

Page *BufferPoolManager::NewPageImpl(page_id_t *page_id) {
  // 0.   Make sure you call DiskManager::AllocatePage!
  // 1.   If all the pages in the buffer pool are pinned, return nullptr.
  // 2.   Pick a victim page P from either the free list or the replacer. Always pick from the free list first.
  // 3.   Update P's metadata, zero out memory and add P to the page table.
  // 4.   Set the page ID output parameter. Return a pointer to P.
  latch_.lock();
  page_id_t tmp_page_id = disk_manager_->AllocatePage();
  // LOG_INFO("NewPageImpl page id is %d" , tmp_page_id );
  frame_id_t tmp_frame_id_ = -1;
  //如果
  if (!free_list_.empty()) {
    tmp_frame_id_ = free_list_.front();
    //  LOG_INFO("NewPageImpl is tmp_frame_id_ %d" , tmp_frame_id_);
    free_list_.pop_front();
  } else {
    if (replacer_->Victim(&tmp_frame_id_)) {
      Page *replace_page = &pages_[tmp_frame_id_];
      if (pages_[tmp_frame_id_].IsDirty()) {
        disk_manager_->WritePage(replace_page->page_id_, pages_[tmp_frame_id_].GetData());
        replace_page->pin_count_ = 0;
      }
      page_table_.erase(replace_page->page_id_);
      pages_[tmp_frame_id_].page_id_ = INVALID_PAGE_ID;
      pages_[tmp_frame_id_].ResetMemory();
    }
  }

  if (tmp_frame_id_ != -1) {
    *page_id = tmp_page_id;
    // LOG_INFO("NewPageImpl page id after is %d" , *page_id);
    Page *page = &pages_[tmp_frame_id_];
    page->is_dirty_ = false;
    page->page_id_ = *page_id;
    page->pin_count_ = 1;

    disk_manager_->WritePage(*page_id, pages_[tmp_frame_id_].data_);
    page_table_.insert(std::pair<page_id_t, frame_id_t>(tmp_page_id, tmp_frame_id_));
    //  LOG_INFO("NewPageImpl is  after tmp_frame_id_ %d" , tmp_frame_id_);

    latch_.unlock();
    return page;
  }
  latch_.unlock();
  return nullptr;
}

bool BufferPoolManager::DeletePageImpl(page_id_t page_id) {
  // 0.   Make sure you call DiskManager::DeallocatePage!
  // 1.   Search the page table for the requested page (P).
  // 1.   If P does not exist, return true.
  // 2.   If P exists, but has a non-zero pin-count, return false. Someone is using the page.
  // 3.   Otherwise, P can be deleted. Remove P from the page table, reset its metadata and return it to the free list.
  latch_.lock();
  // do step 0
  //disk_manager_->DeallocatePage(page_id);
  // do step 1
  if (page_table_.count(page_id) == 0) {
    latch_.unlock();
    return true;
  }
  // do step  1.1
  if (pages_[page_table_[page_id]].pin_count_ != 0) {
    latch_.unlock();
    return false;
  }
  frame_id_t tmp_frame_id_ = page_table_[page_id];
  if (pages_[page_table_[page_id]].IsDirty()) {
    FetchPageImpl(page_id);
  }
  disk_manager_->DeallocatePage(page_id);
  pages_[page_table_[page_id]].ResetMemory();
  pages_[page_table_[page_id]].page_id_ = INVALID_PAGE_ID;
  pages_[page_table_[page_id]].is_dirty_ = false;
  page_table_.erase(page_id);

  free_list_.push_back(tmp_frame_id_);
  latch_.unlock();
  return true;
}

void BufferPoolManager::FlushAllPagesImpl() {
  // You can do it!
  latch_.lock();
  for (auto i : page_table_) {
    disk_manager_->WritePage(i.first, pages_[i.second].GetData());
  }
  latch_.unlock();
}

}  // namespace bustub
