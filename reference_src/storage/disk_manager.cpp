#include <stdexcept>
#include <sys/stat.h>

#include "glog/logging.h"
#include "page/bitmap_page.h"
#include "storage/disk_manager.h"

DiskManager::DiskManager(const std::string &db_file) : file_name_(db_file) {
  std::scoped_lock<std::recursive_mutex> lock(db_io_latch_);
  db_io_.open(db_file, std::ios::binary | std::ios::in | std::ios::out);
  // directory or file does not exist
  if (!db_io_.is_open()) {
    db_io_.clear();
    // create a new file
    db_io_.open(db_file, std::ios::binary | std::ios::trunc | std::ios::out);
    db_io_.close();
    // reopen with original mode
    db_io_.open(db_file, std::ios::binary | std::ios::in | std::ios::out);
    if (!db_io_.is_open()) {
      throw std::exception();
    }
  }
  ReadPhysicalPage(META_PAGE_ID, meta_data_);
  ReadPhysicalPage(MapPageId(0)-1,cur_bitmap_);
}

void DiskManager::Close() {
  WritePage(META_PAGE_ID, meta_data_);
  WritePage(MapPageId(extent_id_*BitmapPage<PAGE_SIZE>::GetMaxSupportedSize())-1,cur_bitmap_);
  std::scoped_lock<std::recursive_mutex> lock(db_io_latch_);
  if (!closed) {
    db_io_.close();
    closed = true;
  }
}

void DiskManager::ReadPage(page_id_t logical_page_id, char *page_data) {
  ASSERT(logical_page_id >= 0, "Invalid page id.");
  ReadPhysicalPage(MapPageId(logical_page_id), page_data);
}

void DiskManager::WritePage(page_id_t logical_page_id, const char *page_data) {
  ASSERT(logical_page_id >= 0, "Invalid page id.");
  WritePhysicalPage(MapPageId(logical_page_id), page_data);
}

page_id_t DiskManager::AllocatePage() {
  /*get the Meta page*/
  static BitmapPage<PAGE_SIZE> *page;
  /*compute each extent Size*/
  DiskFileMetaPage *meta_page = reinterpret_cast<DiskFileMetaPage *>(GetMetaData());

  page_id_t allocate_id=-1;
  if (meta_page->num_allocated_pages_ > MAX_VALID_PAGE_ID) /*there is no valid page*/
  return INVALID_PAGE_ID;
  else {
    bool find = false;
    uint32_t free_extent_id = -1;
    for (free_extent_id = 0; free_extent_id < meta_page->num_extents_; free_extent_id++) {
      if (meta_page->extent_used_page_[free_extent_id] < (page->GetMaxSupportedSize())) {
        find = true;
        break;
      }
    }
    if (!find) {
      /*if there is no valid extents, return invalid_page_id*/
      if (meta_page->num_extents_ == MAX_EXTENT_NUM) return INVALID_PAGE_ID;
      meta_page->num_extents_++;
      meta_page->extent_used_page_[meta_page->num_extents_ - 1] = 0;
      free_extent_id = meta_page->num_extents_ - 1;
    }
    
    uint32_t page_offset;
    if(free_extent_id!=extent_id_){
        WritePage(MapPageId(extent_id_ * page->GetMaxSupportedSize()) - 1,cur_bitmap_);
        ReadPage(MapPageId(free_extent_id * page->GetMaxSupportedSize()) - 1, cur_bitmap_);
        extent_id_=free_extent_id;
    }
    /*get the i th bitmap*/
    page = reinterpret_cast<BitmapPage<PAGE_SIZE> *>(cur_bitmap_);
    page->AllocatePage(page_offset);

    allocate_id = page_offset + BitmapPage<PAGE_SIZE>::GetMaxSupportedSize() * free_extent_id;
    (meta_page->extent_used_page_[free_extent_id])++;
    (meta_page->num_allocated_pages_)++;
    /*write back the bitmap, use cur_bitmap_, we don't need to write back*/
    //WritePhysicalPage(MapPageId(free_extent_id * page->GetMaxSupportedSize()) - 1, reinterpret_cast<char *>(page));
    return allocate_id;
  }
}

void DiskManager::DeAllocatePage(page_id_t logical_page_id) {
  uint32_t extents_id = logical_page_id / BitmapPage<PAGE_SIZE>::GetMaxSupportedSize();
  uint32_t page_id = logical_page_id % BitmapPage<PAGE_SIZE>::GetMaxSupportedSize();

  DiskFileMetaPage *meta_page = reinterpret_cast<DiskFileMetaPage *>(GetMetaData());
  static BitmapPage<PAGE_SIZE> *bitmapPage;
  //static char bitmap[PAGE_SIZE];
  if(extents_id!=extent_id_){
    WritePage(MapPageId(extent_id_ * BitmapPage<PAGE_SIZE>::GetMaxSupportedSize()) - 1,cur_bitmap_);
    ReadPage(MapPageId(extents_id * BitmapPage<PAGE_SIZE>::GetMaxSupportedSize()) - 1,
      cur_bitmap_);
    extent_id_ = extents_id;
  }
   bitmapPage = reinterpret_cast<BitmapPage<PAGE_SIZE> *>(cur_bitmap_);
    bitmapPage->DeAllocatePage(page_id);
    
    meta_page->extent_used_page_[extents_id]--;
    meta_page->num_allocated_pages_--;
    //WritePhysicalPage(MapPageId(extents_id * BitmapPage<PAGE_SIZE>::GetMaxSupportedSize()) - 1,
                      //bitmap);
}

bool DiskManager::IsPageFree(page_id_t logical_page_id) {

  uint32_t extents_id = logical_page_id / BitmapPage<PAGE_SIZE>::GetMaxSupportedSize();
  uint32_t page_id = logical_page_id % BitmapPage<PAGE_SIZE>::GetMaxSupportedSize();
  static BitmapPage<PAGE_SIZE> *bitmapPage;
  //static char bitmap[PAGE_SIZE];
  if(extents_id!=extent_id_){
    WritePage(MapPageId(extent_id_ * BitmapPage<PAGE_SIZE>::GetMaxSupportedSize()) - 1,cur_bitmap_);
    ReadPage(MapPageId(extents_id * BitmapPage<PAGE_SIZE>::GetMaxSupportedSize()) - 1, cur_bitmap_);
    extent_id_=extents_id;
  }
  bitmapPage = reinterpret_cast<BitmapPage<PAGE_SIZE> *>(cur_bitmap_);
  return bitmapPage->IsPageFree(page_id);
}

page_id_t DiskManager::MapPageId(page_id_t logical_page_id) {
  /*each extents size contain entents_Size pages*/
  const size_t extents_Size = BitmapPage<PAGE_SIZE>::GetMaxSupportedSize() + 1;

  const size_t logical_Size = BitmapPage<PAGE_SIZE>::GetMaxSupportedSize();
  uint32_t extent_num = logical_page_id / logical_Size;
  uint32_t extent_offset = logical_page_id % logical_Size;
  /*meta page and bitmap page*/
  page_id_t physical_page_id = extent_num * extents_Size + 1 + 1+extent_offset;
  return physical_page_id;
}

int DiskManager::GetFileSize(const std::string &file_name) {
  struct stat stat_buf;
  int rc = stat(file_name.c_str(), &stat_buf);
  return rc == 0 ? stat_buf.st_size : -1;
}

void DiskManager::ReadPhysicalPage(page_id_t physical_page_id, char *page_data) {
  int offset = physical_page_id * PAGE_SIZE;
  // check if read beyond file length
  if (offset >= GetFileSize(file_name_)) {
#ifdef ENABLE_BPM_DEBUG
    LOG(INFO) << "Read less than a page" << std::endl;
#endif
    memset(page_data, 0, PAGE_SIZE);
  } else {
    // set read cursor to offset
    db_io_.seekp(offset);
    db_io_.read(page_data, PAGE_SIZE);
    // if file ends before reading PAGE_SIZE
    int read_count = db_io_.gcount();
    if (read_count < PAGE_SIZE) {
#ifdef ENABLE_BPM_DEBUG
      LOG(INFO) << "Read less than a page" << std::endl;
#endif
      memset(page_data + read_count, 0, PAGE_SIZE - read_count);
    }
  }
}

void DiskManager::WritePhysicalPage(page_id_t physical_page_id, const char *page_data) {
  size_t offset = static_cast<size_t>(physical_page_id) * PAGE_SIZE;
  // set write cursor to offset
  db_io_.seekp(offset);
  db_io_.write(page_data, PAGE_SIZE);
  // check for I/O error
  if (db_io_.bad()) {
    LOG(ERROR) << "I/O error while writing";
    return;
  }
  // needs to flush to keep disk file in sync
  db_io_.flush();
}