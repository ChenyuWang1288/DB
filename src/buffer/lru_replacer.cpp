#include "buffer/lru_replacer.h"
#include <unordered_map>
#include <algorithm>
using namespace std;
LRUReplacer::LRUReplacer(size_t num_pages) {

}

LRUReplacer::~LRUReplacer() = default;

bool LRUReplacer::Victim(frame_id_t *frame_id) {
  if (Size() > 0) {
    *frame_id = lru_list_.front();  // 返回最近最少访问的元素，即链表的第一个元素
    lru_list_.pop_front();
    return true;
  }
  return false;
}

void LRUReplacer::Pin(frame_id_t frame_id) 
{ 
  // 从lru_list_中移除数据页
  list<frame_id_t>::iterator list_iter1 = find(lru_list_.begin(), lru_list_.end(), frame_id);
  if (list_iter1 != lru_list_.end()) {
	// 找到了该数据页，删除
    lru_list_.remove(*list_iter1);
  }
}

void LRUReplacer::Unpin(frame_id_t frame_id) { 
  
  list<frame_id_t>::iterator list_iter1 = find(lru_list_.begin(), lru_list_.end(), frame_id);
  if (list_iter1 == lru_list_.end()) {
    // 没找到该数据页，此时把它加入lru_list_
    lru_list_.push_back(frame_id);
  }
}

size_t LRUReplacer::Size() {
    // 即lru_list_中元素的数量
  return lru_list_.size();
  return 0;
}