#include "buffer/lru_replacer.h"

LRUReplacer::LRUReplacer(size_t num_pages) :max_size(num_pages){

}

LRUReplacer::~LRUReplacer() = default;

bool LRUReplacer::Victim(frame_id_t *frame_id) { 
 if (max_size==0||lru_list.size()==0) {
    return false; /*all the pages are pinned or there is no pages in lru_list*/
 }
 else {
   auto it_last = prev(lru_list.end()); /*the last iterator in the list*/
   *frame_id = *it_last;
   lru_list.erase(it_last); /*delete the last element*/
   auto it_map = id_map_it.find(*frame_id); 
   id_map_it.erase(it_map);/*delete the mapping info*/
   return true;
 }
}

void LRUReplacer::Pin(frame_id_t frame_id) { 
    auto it_map = id_map_it.find(frame_id);
  if (it_map==id_map_it.end()) {
      return; /*if this frame is not in this list, nothing happens*/
  }
  lru_list.erase(it_map->second);
    id_map_it.erase(it_map);
}
 
void LRUReplacer::Unpin(frame_id_t frame_id) { 
    /*I don't know what unpin is*/ 
    auto it_map = id_map_it.find(frame_id);
    if (it_map == id_map_it.end())
    {
      /*the page is not in this list*/
      Access(frame_id);
    
    } else {
      /*the page is in this list*/
      Access(frame_id);
    }
}

size_t LRUReplacer::Size() {
  return lru_list.size();
}

void LRUReplacer::Access(frame_id_t frame_id) { 
	auto it_map = id_map_it.find(frame_id);
	if (it_map == id_map_it.end()) {
          lru_list.push_front(frame_id); /*if can't find this frame_id*/
          id_map_it.insert(pair<frame_id_t, list<frame_id_t>::iterator>
              (frame_id, lru_list.begin()));
    } 
    else
    {
      lru_list.erase(it_map->second); /*delete the element and push it to the begin() of the list*/
      lru_list.push_front(frame_id);
      auto it_list=lru_list.begin();
      it_map->second = it_list; /*modify the iterator in map*/
    }

}