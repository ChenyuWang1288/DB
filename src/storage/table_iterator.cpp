#include "common/macros.h"
#include "storage/table_iterator.h"
#include "storage/table_heap.h"

//TableIterator::TableIterator(){
//
//}

TableIterator::TableIterator(const TableIterator &other):row_(other.row_),cur_page_(other.cur_page_),table_heap_(other.table_heap_),txn_(other.txn_){

}

TableIterator::TableIterator(Row row, TablePage *table_page, TableHeap *table_heap, Transaction *txn):row_(row),cur_page_(table_page), table_heap_(table_heap),txn_(txn) {

}

//TableIterator::~TableIterator() {
//
//}
/*
bool TableIterator::operator==(const TableIterator &itr) const { return row_.GetRowId() == itr.row_.GetRowId(); }

bool TableIterator::operator!=(const TableIterator &itr) const { return !(itr == (*this)); } */

const Row &TableIterator::operator*() { return row_; }

Row *TableIterator::operator->() { return &row_; }

TableIterator &TableIterator::operator++() { 
  
  if (cur_page_ == nullptr) {
    LOG(WARNING) << "Try to ++ iterator end()" << std::endl;
    return *this;//this is TableHeap.end()
  }
  /*if this row is the last row in this page*/
  RowId next_rid;
  if (!cur_page_->GetNextTupleRid(row_.rid_, &next_rid)) {
    if (cur_page_ ->GetNextPageId()==INVALID_PAGE_ID) {
      /*this is a .end()*/
      row_.SetRowId(INVALID_ROWID);
      cur_page_ = nullptr;
      return *this;
    }
    cur_page_ = reinterpret_cast<TablePage *>(table_heap_->buffer_pool_manager_->FetchPage(cur_page_->GetNextPageId()));
    if (cur_page_ == nullptr) {
      LOG(WARNING) << "Fetch page fails when iterator ++" << std ::endl;
    }
    /*else, cur_page_ is the next page*/
    while (!cur_page_->GetFirstTupleRid(&next_rid)) {
      if (cur_page_->GetNextPageId()==INVALID_PAGE_ID) {
        cur_page_ = nullptr;
        row_.SetRowId(INVALID_ROWID);
        return *this;
      }
      table_heap_->buffer_pool_manager_->UnpinPage(cur_page_->GetPageId(),false);
      /*go to next page until we get to the last page or find a valid first rid*/
      cur_page_ =
          reinterpret_cast<TablePage *>(table_heap_->buffer_pool_manager_->FetchPage(cur_page_->GetNextPageId()));
      if (cur_page_ == nullptr) {
        LOG(WARNING) << "Fetch page fails when iterator ++" << std ::endl;
        return *this;
      }
    }
  } 
  /*else we get next tuple next_rid*/
  /*get the tuple*/
  row_.SetRowId(next_rid);
  if (!cur_page_->GetTuple(&row_, table_heap_->schema_, txn_, nullptr)) {
    LOG(WARNING) << "Get tuple fails while ++iterator" << std ::endl;
    row_.SetRowId(INVALID_ROWID);
    cur_page_ = nullptr;
  }
  return *this;
 
}

TableIterator TableIterator::operator++(int) { 
  /*get a copy of current iterator as return value*/
  TableIterator ret = TableIterator(*this);
  ++(*this);
  return TableIterator(ret);
}
