#include "index/basic_comparator.h"
#include "index/generic_key.h"
#include "index/index_iterator.h"

INDEX_TEMPLATE_ARGUMENTS INDEXITERATOR_TYPE::IndexIterator(LeafPage *target_leaf, int index,
                                                           BufferPoolManager *buffer_pool_manager):target_leaf_(target_leaf),index_(index),buffer_pool_manager_(buffer_pool_manager) {

}

INDEX_TEMPLATE_ARGUMENTS INDEXITERATOR_TYPE::IndexIterator(page_id_t leaf_page, int position,
                                                           BufferPoolManager *buffer_pool_manager)
    : target_leaf_(nullptr), index_(position), buffer_pool_manager_(buffer_pool_manager) {
  target_leaf_ = reinterpret_cast<LeafPage *>(buffer_pool_manager_->FetchPage(leaf_page)->GetData());
}

INDEX_TEMPLATE_ARGUMENTS INDEXITERATOR_TYPE::~IndexIterator() {
  buffer_pool_manager_->UnpinPage(target_leaf_->GetPageId(), true);
}

INDEX_TEMPLATE_ARGUMENTS const MappingType &INDEXITERATOR_TYPE::operator*() { 
  return target_leaf_->GetItem(index_); 
}

INDEX_TEMPLATE_ARGUMENTS INDEXITERATOR_TYPE &INDEXITERATOR_TYPE::operator++() {
  if (index_ < target_leaf_->GetSize()-1){
    index_++;
  } else {
    /*find the next leaf*/
    if (target_leaf_->GetNextPageId() != INVALID_PAGE_ID) {
      LeafPage* next_leaf = reinterpret_cast<LeafPage *>(buffer_pool_manager_->FetchPage(target_leaf_->GetNextPageId())->GetData());
      buffer_pool_manager_->UnpinPage(target_leaf_->GetPageId(),true);
      target_leaf_ = next_leaf;
      index_ = 0;
    } else {
      /*no next leaf*/
      buffer_pool_manager_->UnpinPage(target_leaf_->GetPageId(),true);
      target_leaf_=nullptr;
      index_ = 0;
    }
  }
  return *this;
}

INDEX_TEMPLATE_ARGUMENTS
bool INDEXITERATOR_TYPE::operator==(const IndexIterator &itr) const {
  return (itr.target_leaf_ == target_leaf_) && (itr.index_ == index_);
}

INDEX_TEMPLATE_ARGUMENTS
bool INDEXITERATOR_TYPE::operator!=(const IndexIterator &itr) const { 
  return !(itr == (*this)); 
}

template
class IndexIterator<int, int, BasicComparator<int>>;

template
class IndexIterator<GenericKey<4>, RowId, GenericComparator<4>>;

template
class IndexIterator<GenericKey<8>, RowId, GenericComparator<8>>;

template
class IndexIterator<GenericKey<16>, RowId, GenericComparator<16>>;

template
class IndexIterator<GenericKey<32>, RowId, GenericComparator<32>>;

template
class IndexIterator<GenericKey<64>, RowId, GenericComparator<64>>;
