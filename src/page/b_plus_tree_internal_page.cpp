#include "index/basic_comparator.h"
#include "index/generic_key.h"
#include "page/b_plus_tree_internal_page.h"

/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/
/*
 * Init method after creating a new internal page
 * Including set page type, set current size, set page id, set parent id and set
 * max page size
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Init(page_id_t page_id, page_id_t parent_id, int max_size) { 
  
  SetPageId(page_id);
  SetParentPageId(parent_id);
  SetMaxSize(max_size);
  SetSize(0);
  SetPageType(IndexPageType::INTERNAL_PAGE);
}
/*
 * Helper method to get/set the key associated with input "index"(a.k.a
 * array offset)
 * (also known as) array offset
 */
INDEX_TEMPLATE_ARGUMENTS
KeyType B_PLUS_TREE_INTERNAL_PAGE_TYPE::KeyAt(int index) const { 
  return array_[index].first;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::SetKeyAt(int index, const KeyType &key) { 
  array_[index].first = key; 
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::SetValueAt(int index, const ValueType &value) { 
  array_[index].second = value; 
}
    /*
 * Helper method to find and return array index(or offset), so that its value
 * equals to input "value"
 */
INDEX_TEMPLATE_ARGUMENTS
int B_PLUS_TREE_INTERNAL_PAGE_TYPE::ValueIndex(const ValueType &value) const {
  /*notice: Value has no order, so linear search*/
  int ret_index = -1;
  for (int i = 0; i < GetSize(); i++) {
    if (array_[i].second == value) {
      ret_index = i;
      break;
    }
  }
  return ret_index;
}

/*
 * Helper method to get the value associated with input "index"(a.k.a array
 * offset)
 */
INDEX_TEMPLATE_ARGUMENTS
ValueType B_PLUS_TREE_INTERNAL_PAGE_TYPE::ValueAt(int index) const { 
  return array_[index].second; 
}

/*****************************************************************************
 * LOOKUP
 *****************************************************************************/
/*
 * Find and return the child pointer(page_id) which points to the child page
 * that contains input "key"
 * Start the search from the second key(the first key should always be invalid)
 */
INDEX_TEMPLATE_ARGUMENTS
ValueType B_PLUS_TREE_INTERNAL_PAGE_TYPE::Lookup(const KeyType &key, const KeyComparator &comparator) const { 
  /*notice: key has an order: binary search*/
  /*we need to find the first element > key*/
  /*suppose we always keep the minimum key in array_[0].first*/
  int left = 0;
  int right = GetSize();
  int mid = -1;

  while (left < right) {
    mid = (left + right) / 2;
    if (comparator(key, array_[mid].first) >= 0) {
      left = mid + 1;
    } else {
      right = mid;
    }
  }
  return array_[left - 1].second;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Populate new root page with old_value + new_key & new_value
 * When the insertion cause overflow from leaf page all the way upto the root
 * page, you should create a new root page and populate its elements.
 * NOTE: This method is only called within InsertIntoParent()(b_plus_tree.cpp)
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::PopulateNewRoot(const ValueType &old_value, const KeyType &new_key,
                                                    const ValueType &new_value) {
  /*I choose to store the valid information in array_[0].first. So this API is
  replaced by code in InsertIntoParent()(b_plus_tree.cpp)
  */
  return;
}

/*
 * Insert new_key & new_value pair right after the pair with its value ==
 * old_value
 * @return:  new size after insertion
 */
INDEX_TEMPLATE_ARGUMENTS
int B_PLUS_TREE_INTERNAL_PAGE_TYPE::InsertNodeAfter(const ValueType &old_value, const KeyType &new_key,
                                                    const ValueType &new_value) {
  /*find the index*/
  int old_index = ValueIndex(old_value);
  for (int i = GetSize(); i > old_index + 1; i--) {
    array_[i] = array_[i - 1];
  }
  array_[old_index + 1] = MappingType(new_key, new_value);
  SetSize(GetSize() + 1);
  return GetSize();
}

/*****************************************************************************
 * SPLIT
 *****************************************************************************/
/*
 * Remove half of key & value pairs from this page to "recipient" page
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveHalfTo(BPlusTreeInternalPage* recipient,
                                                BufferPoolManager *buffer_pool_manager) {
  /*here I always use this function when there are maxsize+1 elements*/ 
  recipient->CopyNFrom(&array_[GetMinSize()], GetMaxSize() - GetMinSize() + 1, buffer_pool_manager);
  SetSize(GetMinSize());
}

/* Copy entries into me, starting from {items} and copy {size} entries.
 * Since it is an internal page, for all entries (pages) moved, their parents page now changes to me.
 * So I need to 'adopt' them by changing their parent page id, which needs to be persisted with BufferPoolManger
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::CopyNFrom(MappingType *items, int size, BufferPoolManager *buffer_pool_manager) {
  /*always call this function when array is empty*/
  for (int i = 0; i < size; i++) {
    array_[i] = items[i];
  }
  SetSize(size);
  /*adopt all the children*/
  for (int i = 0; i < size; i++) {
    BPlusTreePage *page = reinterpret_cast<BPlusTreePage *>(buffer_pool_manager->FetchPage(ValueAt(i))->GetData());
    page->SetParentPageId(GetPageId());
    buffer_pool_manager->UnpinPage(page->GetPageId(),true);
  }
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * Remove the key & value pair in internal page according to input index(a.k.a
 * array offset)
 * NOTE: store key&value pair continuously after deletion
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Remove(int index) {
  for (int i = index; i < GetSize()-1; i++) {
    array_[i] = array_[i + 1];
  }
  SetSize(GetSize() - 1);
}

/*
 * Remove the only key & value pair in internal page and return the value
 * NOTE: only call this method within AdjustRoot()(in b_plus_tree.cpp)
 */
INDEX_TEMPLATE_ARGUMENTS
ValueType B_PLUS_TREE_INTERNAL_PAGE_TYPE::RemoveAndReturnOnlyChild() {
  // replace with your own code
  ValueType val{};
  return val;
}

/*****************************************************************************
 * MERGE
 *****************************************************************************/
/*
 * Remove all of key & value pairs from this page to "recipient" page.
 * The middle_key is the separation key you should get from the parent. You need
 * to make sure the middle key is added to the recipient to maintain the invariant.
 * You also need to use BufferPoolManager to persist changes to the parent page id for those
 * pages that are moved to the recipient
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveAllTo(BPlusTreeInternalPage *recipient, const KeyType &middle_key,
                                               BufferPoolManager *buffer_pool_manager) {
  /*we always assume recipient is the previous sibling of this node*/
  /*we don't need middle_key because we store the key in array[0].first*/
  for (int i = 0; i < GetSize(); i++) {
    recipient->array_[i + recipient->GetSize()] = array_[i];
  }
  /*recipient will adopt all the children*/
  for (int i = 0; i < GetSize(); i++) {
    BPlusTreePage *page = reinterpret_cast<BPlusTreePage *>(
        buffer_pool_manager->FetchPage(recipient->ValueAt(i + recipient->GetSize()))->GetData());
    page->SetParentPageId(recipient->GetPageId());
    buffer_pool_manager->UnpinPage(page->GetPageId(), true);
  }
  recipient->SetSize(recipient->GetSize() + GetSize());
}

/*****************************************************************************
 * REDISTRIBUTE
 *****************************************************************************/
/*
 * Remove the first key & value pair from this page to tail of "recipient" page.
 *
 * The middle_key is the separation key you should get from the parent. You need
 * to make sure the middle key is added to the recipient to maintain the invariant.
 * You also need to use BufferPoolManager to persist changes to the parent page id for those
 * pages that are moved to the recipient
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveFirstToEndOf(BPlusTreeInternalPage *recipient, const KeyType &middle_key,
                                                      BufferPoolManager *buffer_pool_manager) {
  /*the same as Move all, we have maintain the value in array[0].first*/
  recipient->array_[recipient->GetSize()] = array_[0];
  recipient->SetSize(recipient->GetSize() + 1);
  for (int i = GetSize() - 1; i > 0; i--) {
    array_[i - 1] = array_[i];
  }
  SetSize(GetSize() - 1);
  /*adopt the child*/
  BPlusTreePage *page = reinterpret_cast<BPlusTreePage *>(
      buffer_pool_manager->FetchPage(recipient->array_[recipient->GetSize() - 1].second)->GetData());
  page->SetParentPageId(recipient->GetPageId());
  buffer_pool_manager->UnpinPage(page->GetPageId(), true);
}

/* Append an entry at the end.
 * Since it is an internal page, the moved entry(page)'s parent needs to be updated.
 * So I need to 'adopt' it by changing its parent page id, which needs to be persisted with BufferPoolManger
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::CopyLastFrom(const MappingType &pair, BufferPoolManager *buffer_pool_manager) {

}

/*
 * Remove the last key & value pair from this page to head of "recipient" page.
 * You need to handle the original dummy key properly, e.g. updating recipient’s array to position the middle_key at the
 * right place.
 * You also need to use BufferPoolManager to persist changes to the parent page id for those pages that are
 * moved to the recipient
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveLastToFrontOf(BPlusTreeInternalPage *recipient, const KeyType &middle_key,
                                                      BufferPoolManager *buffer_pool_manager) {
  for (int i = recipient->GetSize(); i > 0; i--) {
    recipient->array_[i] = recipient->array_[i - 1];
  }
  recipient->array_[0] = array_[GetSize() - 1];
  recipient->SetSize(recipient->GetSize() + 1);
  SetSize(GetSize() - 1);
  /*adopt the child*/
  BPlusTreePage *page = reinterpret_cast<BPlusTreePage *>(
      buffer_pool_manager->FetchPage(recipient->array_[0].second)->GetData());
  page->SetParentPageId(recipient->GetPageId());
  buffer_pool_manager->UnpinPage(page->GetPageId(), true);
}

/* Append an entry at the beginning.
 * Since it is an internal page, the moved entry(page)'s parent needs to be updated.
 * So I need to 'adopt' it by changing its parent page id, which needs to be persisted with BufferPoolManger
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::CopyFirstFrom(const MappingType &pair, BufferPoolManager *buffer_pool_manager) {

}

template
class BPlusTreeInternalPage<int, int, BasicComparator<int>>;

template
class BPlusTreeInternalPage<GenericKey<4>, page_id_t, GenericComparator<4>>;

template
class BPlusTreeInternalPage<GenericKey<8>, page_id_t, GenericComparator<8>>;

template
class BPlusTreeInternalPage<GenericKey<16>, page_id_t, GenericComparator<16>>;

template
class BPlusTreeInternalPage<GenericKey<32>, page_id_t, GenericComparator<32>>;

template
class BPlusTreeInternalPage<GenericKey<64>, page_id_t, GenericComparator<64>>;