#include "buffer/buffer_pool_manager.h"
#include "glog/logging.h"
#include "page/bitmap_page.h"

BufferPoolManager::BufferPoolManager(size_t pool_size, DiskManager *disk_manager)
        : pool_size_(pool_size), disk_manager_(disk_manager) {
  pages_ = new Page[pool_size_];
  replacer_ = new LRUReplacer(pool_size_);
  for (size_t i = 0; i < pool_size_; i++) {
    free_list_.emplace_back(i); 
  }
}

BufferPoolManager::~BufferPoolManager() {
  for (auto page: page_table_) {
    FlushPage(page.first);
  }
  delete[] pages_;
  delete replacer_;
}

Page *BufferPoolManager::FetchPage(page_id_t page_id) {
  // 1.     Search the page table for the requested page (P).
  // 1.1    If P exists, pin it and return it immediately.
  // 1.2    If P does not exist, find a replacement page (R) from either the free list or the replacer.
  //        Note that pages are always found from the free list first.
  // 2.     If R is dirty, write it back to the disk.
  // 3.     Delete R from the page table and insert P.
  // 4.     Update P's metadata, read in the page content from disk, and then return a pointer to P.
  if (page_id == INVALID_PAGE_ID) {
    return nullptr;
  }
  auto page=page_table_.find(page_id);
  if (page!=page_table_.end()) {
    /*page_id is in buffer pool*/
    replacer_->Pin(page->second);//pin in replacer;
    pages_[page->second].pin_count_++;
    return &pages_[page->second];
  }
  else
  {
    /*p is not in buffer pool*/
    frame_id_t replace_frame = INVALID_FRAME_ID;
    if (free_list_.size() != 0){
      /*there is a free frame*/
        auto first_free = free_list_.begin();
        replace_frame=*first_free;
        free_list_.erase(first_free);//use the free frame in free list
    } 
    else {
      /*there is no free frame, use replacer*/
      if (replacer_->Victim(&replace_frame) == false) {
        return nullptr;
      }
      else {
        if (pages_[replace_frame].is_dirty_) {
          FlushPage(pages_[replace_frame].page_id_);
          //write this page to disk
        }
        auto pre_map = page_table_.find(pages_[replace_frame].page_id_);
        /*delete the previous one in page_table*/
        page_table_.erase(pre_map);
      }
    }
    disk_manager_->ReadPage(page_id, pages_[replace_frame].data_);
    replacer_->Pin(replace_frame);
    /*meta data?*/
    pages_[replace_frame].is_dirty_ = false;
    pages_[replace_frame].page_id_ = page_id;
    pages_[replace_frame].pin_count_ = 1;
    /*update page_table_*/
    page_table_.insert(pair<page_id_t, frame_id_t>(page_id, replace_frame));
    return &pages_[replace_frame];
  }

}

Page *BufferPoolManager::NewPage(page_id_t &page_id) {
  // 0.   Make sure you call AllocatePage!
  // 1.   If all the pages in the buffer pool are pinned, return nullptr.
  // 2.   Pick a victim page P from either the free list or the replacer. Always pick from the free list first.
  // 3.   Update P's metadata, zero out memory and add P to the page table.
  // 4.   Set the page ID output parameter. Return a pointer to P.
  page_id_t new_page_id = AllocatePage();
  if (new_page_id == INVALID_PAGE_ID) {
    return nullptr; /*no valid page to allocate by disk manager*/
  }
  frame_id_t replace_frame = INVALID_FRAME_ID;
  if (free_list_.size()!=0) {
    /*if there is a free page in free_list_*/
    auto first_free = free_list_.begin();
    replace_frame = *first_free;
    free_list_.erase(first_free);
  } else {
    /*use replacer,the same like fetch*/
    if (replacer_->Victim(&replace_frame) == false) {
      /*if fails, DeallocatePage,*/
      disk_manager_->DeAllocatePage(new_page_id);
      return nullptr;
    }
    /*if find and this page is dirty, write back to disk*/
    if (pages_[replace_frame].is_dirty_) FlushPage(pages_[replace_frame].page_id_);
    auto pre_map = page_table_.find(pages_[replace_frame].page_id_);
    /*delete the previous one in page_table*/
    page_table_.erase(pre_map);
  }
  /*zero memory*/
  pages_[replace_frame].ResetMemory();
  /*meta data of pages*/
  pages_[replace_frame].is_dirty_ = false;
  pages_[replace_frame].page_id_ = new_page_id;
  pages_[replace_frame].pin_count_ = 1;
  /*lru_replacer to set this frame to first*/
  replacer_->Pin(replace_frame);
  /*update page_table*/
  page_table_.insert(pair<page_id_t, frame_id_t>(new_page_id, replace_frame));
  page_id = new_page_id;
  return &pages_[replace_frame];
}

bool BufferPoolManager::DeletePage(page_id_t page_id) {
  // 0.   Make sure you call DeallocatePage!
  // 1.   Search the page table for the requested page (P).
  // 1.   If P does not exist, return true.
  // 2.   If P exists, but has a non-zero pin-count, return false. Someone is using the page.
  // 3.   Otherwise, P can be deleted. Remove P from the page table, reset its metadata and return it to the free list
  
  auto page=page_table_.find(page_id);//page->second is the frame_id
  if (page == page_table_.end()) {
    return true;
  }
  if (pages_[page->second].GetPinCount() != 0) return false;
  ///*if this page is dirty, we need to write it back to disk*/
  //if (pages_[page->second].is_dirty_) {
  //  if (!FlushPage(page_id)) {
  //    LOG(WARNING) << "Delete page error when flush page back in disk manager" << std::endl;
  //  }
  //}
  DeallocatePage(page->first);
  /*reset meta data*/
  pages_[page->second].is_dirty_ = false;
  pages_[page->second].page_id_ = INVALID_PAGE_ID;
  /*add it to free_list_*/
  free_list_.emplace_back(page->second);
  /*delete it in page_table_*/
  page_table_.erase(page);
  
  return true;
}

bool BufferPoolManager::UnpinPage(page_id_t page_id, bool is_dirty) {
  //1 find whether page_id is in this buffer pool, if not, return false
  //2 find whether this page pin_count is more than 1, if it is, just set is_dirty_,and decrement pin_count
  //3 if pin_count is 1, unpin it and add it to lrulist by replacer
  auto page = page_table_.find(page_id);
  if (page == page_table_.end()) return false;
  if(--pages_[page->second].pin_count_<0) {
    pages_[page->second].pin_count_ = 0;
  }
  pages_[page->second].is_dirty_ = pages_[page->second].is_dirty_ || is_dirty;
  if (pages_[page->second].GetPinCount() ==0) {
    replacer_->Unpin(page->second);
  } 
  return true;
}

bool BufferPoolManager::FlushPage(page_id_t page_id) {
  //1 if the page_id is not allocated, return false
  //2 if the page_id is not in buffer pool return false;
  //3 find frame_id and write
  

  /*I don't know why we don't need this check */
  if (disk_manager_->IsPageFree(page_id)) return false;
  auto page = page_table_.find(page_id);
  if (page == page_table_.end()) return false;
  disk_manager_->WritePage(page_id, pages_[page->second].GetData());
  return true;
}
//bool BufferPoolManager::FlushAllPages() {
//
//}



page_id_t BufferPoolManager::AllocatePage() {
  int next_page_id = disk_manager_->AllocatePage();
  return next_page_id;
}

void BufferPoolManager::DeallocatePage(page_id_t page_id) {
  disk_manager_->DeAllocatePage(page_id);
}

bool BufferPoolManager::IsPageFree(page_id_t page_id) {
  return disk_manager_->IsPageFree(page_id);
}

// Only used for debug
bool BufferPoolManager::CheckAllUnpinned() {
  bool res = true;
  for (size_t i = 0; i < pool_size_; i++) {
    if (pages_[i].pin_count_ != 0) {
      res = false;
      LOG(ERROR) << "page " << pages_[i].page_id_ << " pin count:" << pages_[i].pin_count_ << endl;
    }
  }
  return res;
}
