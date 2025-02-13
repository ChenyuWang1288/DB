#include "index/b_plus_tree_index.h"
#include "index/generic_key.h"

/*To Update index_roots_page I add a parameter index_roots_page_id*/
INDEX_TEMPLATE_ARGUMENTS
BPLUSTREE_INDEX_TYPE::BPlusTreeIndex(index_id_t index_id, IndexSchema *key_schema,
                                     BufferPoolManager *buffer_pool_manager)
        : Index(index_id, key_schema),
          comparator_(key_schema_),
          container_(index_id, buffer_pool_manager, comparator_) {

}

INDEX_TEMPLATE_ARGUMENTS
dberr_t BPLUSTREE_INDEX_TYPE::InsertEntry(const Row &key, RowId row_id, Transaction *txn) {
  ASSERT(row_id.Get() != INVALID_ROWID.Get(), "Invalid row id for index insert.");
  KeyType index_key;
  index_key.SerializeFromKey(key, key_schema_);

  bool status = container_.Insert(index_key, row_id, txn);

  if (!status) {
    return DB_FAILED;
  }
  return DB_SUCCESS;
}

INDEX_TEMPLATE_ARGUMENTS
dberr_t BPLUSTREE_INDEX_TYPE::RemoveEntry(const Row &key, RowId row_id, Transaction *txn) {
  KeyType index_key;
  index_key.SerializeFromKey(key, key_schema_);

  container_.Remove(index_key, txn);
  return DB_SUCCESS;
}

INDEX_TEMPLATE_ARGUMENTS
dberr_t BPLUSTREE_INDEX_TYPE::ScanKey(const Row &key, vector<RowId> &result, int &position, page_id_t& leaf_page_id, Transaction *txn) {
  /*position is first index in leaf page which [position].key>=key*/
  /*leaf_page_id is the leaf page id for constructor of iterator*/
  KeyType index_key;
  index_key.SerializeFromKey(key, key_schema_);
  if (container_.GetValue(index_key, result, position, leaf_page_id, txn)) {
    return DB_SUCCESS;
  }
  return DB_KEY_NOT_FOUND;
}

INDEX_TEMPLATE_ARGUMENTS
dberr_t BPLUSTREE_INDEX_TYPE::Destroy() {
  container_.Destroy();
  return DB_SUCCESS;
}

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE BPLUSTREE_INDEX_TYPE::GetBeginIterator() {
  return container_.Begin();
}

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE
BPLUSTREE_INDEX_TYPE::GetBeginIterator(const KeyType &key) {
  return container_.Begin(key);
}

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE BPLUSTREE_INDEX_TYPE::GetEndIterator() {
  return container_.End();
}

template
class BPlusTreeIndex<GenericKey<4>, RowId, GenericComparator<4>>;

template
class BPlusTreeIndex<GenericKey<8>, RowId, GenericComparator<8>>;

template
class BPlusTreeIndex<GenericKey<16>, RowId, GenericComparator<16>>;

template
class BPlusTreeIndex<GenericKey<32>, RowId, GenericComparator<32>>;

template
class BPlusTreeIndex<GenericKey<64>, RowId, GenericComparator<64>>;