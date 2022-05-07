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
  if (page_table_.find(page_id) != page_table_.end()) {//内存中如果能找到这个page直接返回就可以
    LRUReplacer->Pin(page_table_(page_id));//pin一下
    Page *p = &pages[page_table_(page_id)];//定义一个指向该结构的指针
    p->pin_count_++;//修改pin的值
    return p;
  } else {//如果在内存中没找到就需要在内存中寻找一个位置存放读取的page
    frame_id_t frame_id_;
    frame_id_t *frame_id;
    if (free_list_.size() != 0) {//如果有自由区
      frame_id_ = free_list_.top();//获得自由区栈顶的元素（在缓冲区的位置id）
      free_list_.pop();//将栈顶元素删掉，此时已经获得写入缓冲区的具体位置
    } else {//如果自由区为空，此时需要在替换区找到需要替换的位置
      bool n = LRUReplacer::Victim(frame_id);//将参数传过去，在LRU中会将替换的id传过来
      if (n == 0)//如果为0，则替换区也没有可以替换的元素了
        return nullptr;//此时返回
      else
        frame_id_ = *frame_id;//如果有替换的元素，则确定了最终替换的位置
    }//到这个步骤已经找到内存中即将被写入数据的位置
    
    Page *R == &pages_[frame_id_];//定义一个指针指向要被替换了的结构
    bool is_dirty = R->IsDirty();//判断他是否是脏文件
    if (is_dirty == 1) {
      R->pin_count_ = 0;  //重置pin位
      disk_manager_->WritePage(R->page_id_, R->data_);
    }  //如果是脏文件则要将他写进磁盘
    page_table_.erase(R->page_id_);
    page_table_.insert(page_id, frame_id_);//找到了p要写入的位置：frame_id并且更新相关的位置信息，下一步就是在磁盘中找到我要的数据。
    char *newdata;
    disk_manager_->ReadPage(page_id, newdata);//在磁盘中读入我要的数据
    pages_[frame_id_].data = newdata;//向内存中写入新数据
    pages_[frame_id_].page_id_ = page_id;
    pages_[frame_id_].pin_count_ ++;//修改pin值
    pages_[frame_id_].is_dirty_ = false.  //重置参数
                 return &pages_[frame_id_];//返回结构地址
  }
  // 1.     Search the page table for the requested page (P).
  // 1.1    If P exists, pin it and return it immediately.
  // 1.2    If P does not exist, find a replacement page (R) from either the free list or the replacer.
  //        Note that pages are always found from the free list first.
  // 2.     If R is dirty, write it back to the disk.
  // 3.     Delete R from the page table and insert P.
  // 4.     Update P's metadata, read in the page content from disk, and then return a pointer to P.
  return nullptr;
}

Page *BufferPoolManager::NewPage(page_id_t &page_id) {
  // 0.   Make sure you call AllocatePage!
  // 1.   If all the pages in the buffer pool are pinned, return nullptr.
  // 2.   Pick a victim page P from either the free list or the replacer. Always pick from the free list first.
  // 3.   Update P's metadata, zero out memory and add P to the page table.
  // 4.   Set the page ID output parameter. Return a pointer to P.
  return nullptr;
}

bool BufferPoolManager::DeletePage(page_id_t page_id) {
  // 0.   Make sure you call DeallocatePage!
  // 1.   Search the page table for the requested page (P).
  // 1.   If P does not exist, return true.
  // 2.   If P exists, but has a non-zero pin-count, return false. Someone is using the page.
  // 3.   Otherwise, P can be deleted. Remove P from the page table, reset its metadata and return it to the free list.
  return false;
}

bool BufferPoolManager::UnpinPage(page_id_t page_id, bool is_dirty) {
  return false;
}

bool BufferPoolManager::FlushPage(page_id_t page_id) {
  return false;
}

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
