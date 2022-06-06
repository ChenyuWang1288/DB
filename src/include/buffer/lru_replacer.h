#ifndef MINISQL_LRU_REPLACER_H
#define MINISQL_LRU_REPLACER_H

#include <list>
#include <mutex>
#include <unordered_set>
#include <vector>
#include<unordered_map>
#include "buffer/replacer.h"
#include "common/config.h"

using namespace std;

/**
 * LRUReplacer implements the Least Recently Used replacement policy.
 */
class LRUReplacer : public Replacer {
public:
  /**
   * Create a new LRUReplacer.
   * @param num_pages the maximum number of pages the LRUReplacer will be required to store
   */
  explicit LRUReplacer(size_t num_pages);

  /**
   * Destroys the LRUReplacer.
   */
  ~LRUReplacer() override;

  bool Victim(frame_id_t *frame_id) override;

  void Pin(frame_id_t frame_id) override;

  void Unpin(frame_id_t frame_id) override;
  /*void Access(frame_id_t frame_id): 
   if a frame_id is accessed, move it to the begin of the lru_list
  */
  
  size_t Size() override;

private:
  // add your own private member variables here
 /*lru_list, always replace the last element of the list*/
 list<frame_id_t> lru_list; 
 size_t max_size; 
 /*frame_id mapping iterator, to quickly find where the element is*/
 unordered_map<frame_id_t, list<frame_id_t>::iterator> id_map_it;
 void Access(frame_id_t frame_id);
};

#endif  // MINISQL_LRU_REPLACER_H
