#include "page/bitmap_page.h"
const unsigned char check_mask[8] = {0x80, 0x40, 0x20, 0x10, 0x08, 0x04, 0x02, 0x01};
const unsigned char deAllocate_mask[8] = {0x7f, 0xbf, 0xdf, 0xef, 0xf7, 0xfb, 0xfd, 0xfe};
const unsigned char Allocate_mask[8] = {0x80, 0x40, 0x20, 0x10, 0x08, 0x04, 0x02, 0x01}; 

template<size_t PageSize>
bool BitmapPage<PageSize>::AllocatePage(uint32_t &page_offset) {
  if (page_allocated_ >= GetMaxSupportedSize())
	return false;
  else {
    page_offset = next_free_page_;
    size_t byte_num = page_offset / 8;
    size_t bit_num = page_offset % 8;

    bytes[byte_num] |= Allocate_mask[bit_num];
    next_free_page_=find_next_free_();
    page_allocated_++;
    return true;
  }
}

template<size_t PageSize>
bool BitmapPage<PageSize>::DeAllocatePage(uint32_t page_offset) {
    size_t byte_num = page_offset / 8;
    size_t bit_num = page_offset % 8;
    if ((bytes[byte_num] << bit_num & check_mask[0] )== 0) return false; /*if the bit is free:*/
    else {
      bytes[byte_num] &= deAllocate_mask[bit_num];
      page_allocated_--;
      if (next_free_page_ == 0xffffffff) next_free_page_ = page_offset;
      return true;
  } 
}

template<size_t PageSize>
bool BitmapPage<PageSize>::IsPageFree(uint32_t page_offset) const {
  if (page_offset >= GetMaxSupportedSize()) {
    return false;
  } 
  else {
    size_t byte_num = page_offset / 8;
    size_t bit_num = page_offset % 8;
    if (((bytes[byte_num] << bit_num) & (check_mask[0]))== 0)
      return true;
    else
      return false;
  }
}

template<size_t PageSize>
bool BitmapPage<PageSize>::IsPageFreeLow(uint32_t byte_index, uint8_t bit_index) const {
  return false;
}
template <size_t PageSize>
uint32_t BitmapPage<PageSize>::find_next_free_() const {
  uint32_t free_page_=0xffffffff;
    for (size_t i = 0; i < MAX_CHARS; i++) {
    if (bytes[i] == 0xff) continue;
    else {
      for (int j=0 ; j < (int)8; j++) {
        if ((bytes[i] & check_mask[j]) == 0) {
          free_page_ = i * 8 + j;
          break;
        }
      }
      break;
    }
  }
    return free_page_;
}



template
class BitmapPage<64>;

template
class BitmapPage<128>;

template
class BitmapPage<256>;

template
class BitmapPage<512>;

template
class BitmapPage<1024>;

template
class BitmapPage<2048>;

template
class BitmapPage<4096>;