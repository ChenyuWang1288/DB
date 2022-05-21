#include <algorithm>
#include "index/basic_comparator.h"
#include "index/generic_key.h"
#include "page/b_plus_tree_leaf_page.h"
#include "glog/logging.h"
/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/

/**
 * Init method after creating a new leaf page
 * Including set page type, set current size to zero, set page id/parent id, set
 * next page id and set max size
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::Init(page_id_t page_id, page_id_t parent_id, int max_size) {
  SetPageId(page_id);
  SetParentPageId(parent_id);
  SetMaxSize(max_size);
  SetSize(0);
  SetNextPageId(INVALID_PAGE_ID);
  SetPageType(IndexPageType::LEAF_PAGE);
}
/**
 * Helper methods to set/get next page id
 */
INDEX_TEMPLATE_ARGUMENTS
page_id_t B_PLUS_TREE_LEAF_PAGE_TYPE::GetNextPageId() const {
  return next_page_id_;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::SetNextPageId(page_id_t next_page_id) { 
  next_page_id_ = next_page_id; 
}

/**
 * Helper method to find the first index i so that array_[i].first >= key
 * NOTE: This method is only used when generating index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
int B_PLUS_TREE_LEAF_PAGE_TYPE::KeyIndex(const KeyType &key, const KeyComparator &comparator) const {
  int left = 0;
  int right = GetSize();
  /*find first key that array_[i].first>=key, the result will be store in left*/
  while (left < right) {
    int mid = (left + right) / 2;
    if (comparator(key, array_[mid].first) > 0) {
      left = mid + 1;
    } else {
      right = mid;
    }
  }
  return left;
  //if left== getsize(),it means all element in array_< key
}
/*
 * Helper method to find and return the key associated with input "index"(a.k.a
 * array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
KeyType B_PLUS_TREE_LEAF_PAGE_TYPE::KeyAt(int index) const {
  return array_[index].first;
}

/*
 * Helper method to find and return the key & value pair associated with input
 * "index"(a.k.a array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
const MappingType &B_PLUS_TREE_LEAF_PAGE_TYPE::GetItem(int index) {
  
  return array_[index];
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Insert key & value pair into leaf page ordered by key
 * @return page size after insertion
 */
INDEX_TEMPLATE_ARGUMENTS
int B_PLUS_TREE_LEAF_PAGE_TYPE::Insert(const KeyType &key, const ValueType &value, const KeyComparator &comparator) {
  /*if (GetSize() == GetMaxSize()) {
    LOG(WARNING) << "Insert into leaf page while the size is maxsize" << std::endl;
    return GetSize();
  }*/

  /*before insertion, caller should make sure the size is less than maxsize*/
  /*we will find the first element greater than key*/
  int left = 0;
  int right = GetSize();
  int mid = -1;

  while (left < right) {
    mid = (left + right) / 2;
    if (comparator(key, array_[mid].first) > 0) {
      left = mid + 1;
    } else {
      right = mid;
    }
  }
  /*the value in left is the result*/
  if (left != GetSize() && comparator(key, array_[left].first) == 0) {
    LOG(WARNING) << "Insert duplicated keys!" << std::endl;
    return -1;
  }
  /*move the element*/
  for (int i = GetSize() - 1; i >= left; i--) {
    array_[i + 1] = array_[i];
  }

  array_[left] = MappingType(key, value);
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
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveHalfTo(BPlusTreeLeafPage *recipient) {
  /*when call this function, there are GetMaxSize()+1 elements*/
  /*set next page id to link these 2 page*/
  recipient->SetNextPageId(GetNextPageId());
  SetNextPageId(recipient->GetPageId());
  /*move [GetMinSize()] TO [GetMaxSize()] into recipient*/
  for (int i = GetMinSize(); i <= GetMaxSize(); i++) {
    recipient->array_[i - GetMinSize()] = array_[i];
  } 
  recipient->SetSize(GetMaxSize() - GetMinSize() + 1);
  SetSize(GetMinSize());
}

/*
 * Copy starting from items, and copy {size} number of elements into me.
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::CopyNFrom(MappingType *items, int size) {
  /*for (int i = 0; i < size; i++) {
    array_[]
  }*/
}

/*****************************************************************************
 * LOOKUP
 *****************************************************************************/
/*
 * For the given key, check to see whether it exists in the leaf page. If it
 * does, then store its corresponding value in input "value" and return true.
 * If the key does not exist, then return false
 */
INDEX_TEMPLATE_ARGUMENTS
bool B_PLUS_TREE_LEAF_PAGE_TYPE::Lookup(const KeyType &key, ValueType &value, const KeyComparator &comparator) const {
  /*find the first element >=key :[left,right) */
  int left = 0;
  int right = GetSize();

  while (left < right) {
    int mid = (left + right) / 2;
    if (comparator(key, array_[mid].first) > 0) {
      left = mid + 1;
    } else {
      right = mid;
    }
  }
  /*now left is the first element >=key*/
  if ((left == GetSize()) || (comparator(key, array_[left].first) != 0)) {
    return false;
  } else {
    value = array_[left].second;
    return true;
  }
  return true;
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * First look through leaf page to see whether delete key exist or not. If
 * exist, perform deletion, otherwise return immediately.
 * NOTE: store key&value pair continuously after deletion
 * @return  page size after deletion
 */
INDEX_TEMPLATE_ARGUMENTS
int B_PLUS_TREE_LEAF_PAGE_TYPE::RemoveAndDeleteRecord(const KeyType &key, const KeyComparator &comparator) {
 
  ValueType value;
  if (Lookup(key, value, comparator)) {
    /*the key exist*/
    int index = KeyIndex(key,comparator);
    /*perform deletion*/
    for (int i = index; i < GetSize(); i++) {
      array_[i] = array_[i + 1];
    }
    SetSize(GetSize() - 1);
  }
  return GetSize();
}

/*****************************************************************************
 * MERGE
 *****************************************************************************/
/*
 * Remove all of key & value pairs from this page to "recipient" page. Don't forget
 * to update the next_page id in the sibling page
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveAllTo(BPlusTreeLeafPage *recipient) {
  /*we always suppose recipent is the previous sibling of this node*/
  /*set next_page_id*/
  recipient->SetNextPageId(GetNextPageId());
  for (int i = 0; i < GetSize(); i++) {
    recipient->array_[i + recipient->GetSize()] = array_[i];
  }
  recipient->SetSize(recipient->GetSize() + GetSize());
  SetSize(0);
}

/*****************************************************************************
 * REDISTRIBUTE
 *****************************************************************************/
/*
 * Remove the first key & value pair from this page to "recipient" page.
 *
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveFirstToEndOf(BPlusTreeLeafPage *recipient) {
  recipient->array_[recipient->GetSize()] = array_[0];
  recipient->SetSize(recipient->GetSize() + 1);
  for (int i = 0; i < GetSize() - 1; i++) {
    array_[i] = array_[i + 1];
  }
  SetSize(GetSize() - 1);
}

/*
 * Copy the item into the end of my item list. (Append item to my array)
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::CopyLastFrom(const MappingType &item) {

}

/*
 * Remove the last key & value pair from this page to "recipient" page.
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveLastToFrontOf(BPlusTreeLeafPage *recipient) {
  for (int i = recipient->GetSize(); i > 0; i--) {
    recipient->array_[i] = recipient->array_[i - 1];
  }
  recipient->SetSize(recipient->GetSize() + 1);

  recipient->array_[0] = array_[GetSize()-1];
  SetSize(GetSize() - 1);
}

/*
 * Insert item at the front of my items. Move items accordingly.
 *
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::CopyFirstFrom(const MappingType &item) {

}

template
class BPlusTreeLeafPage<int, int, BasicComparator<int>>;

template
class BPlusTreeLeafPage<GenericKey<4>, RowId, GenericComparator<4>>;

template
class BPlusTreeLeafPage<GenericKey<8>, RowId, GenericComparator<8>>;

template
class BPlusTreeLeafPage<GenericKey<16>, RowId, GenericComparator<16>>;

template
class BPlusTreeLeafPage<GenericKey<32>, RowId, GenericComparator<32>>;

template
class BPlusTreeLeafPage<GenericKey<64>, RowId, GenericComparator<64>>;