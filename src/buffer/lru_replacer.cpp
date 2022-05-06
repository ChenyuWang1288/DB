#include "buffer/lru_replacer.h"
#include <unordered_map>
#include <algorithm>
using namespace std;
LRUReplacer::LRUReplacer(size_t num_pages) {

}

LRUReplacer::~LRUReplacer() = default;

bool LRUReplacer::Victim(frame_id_t *frame_id) {
  if (Size() > 0) {
    *frame_id = lru_list_.front();  // ����������ٷ��ʵ�Ԫ�أ�������ĵ�һ��Ԫ��
    lru_list_.pop_front();
    return true;
  }
  return false;
}

void LRUReplacer::Pin(frame_id_t frame_id) 
{ 
  // ��lru_list_���Ƴ�����ҳ
  list<frame_id_t>::iterator list_iter1 = find(lru_list_.begin(), lru_list_.end(), frame_id);
  if (list_iter1 != lru_list_.end()) {
	// �ҵ��˸�����ҳ��ɾ��
    lru_list_.remove(*list_iter1);
  }
}

void LRUReplacer::Unpin(frame_id_t frame_id) { 
  
  list<frame_id_t>::iterator list_iter1 = find(lru_list_.begin(), lru_list_.end(), frame_id);
  if (list_iter1 == lru_list_.end()) {
    // û�ҵ�������ҳ����ʱ��������lru_list_
    lru_list_.push_back(frame_id);
  }
}

size_t LRUReplacer::Size() {
    // ��lru_list_��Ԫ�ص�����
  return lru_list_.size();
  return 0;
}