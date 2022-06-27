#include "storage/table_heap.h"
#include"glog/logging.h"
bool TableHeap::InsertTuple(Row &row, Transaction *txn) {
  /*if the tuple is too large(>=page_size),we will return immediately*/
  if (row.GetSerializedSize(schema_) > TablePage::MaxTupleSize()) {
    LOG(WARNING) << "The inserted tuple size is too large" << std::endl;
    return false;
  }
  /*first fit strategy*/
  /*from the first page, we use buffer pool to get the next page, ... 
  until we find a invalid page or find a page which could be used to insert tuple*/
  page_id_t page_Id = first_page_id_;
  TablePage *this_page = nullptr; 
  for (; page_Id != INVALID_PAGE_ID; page_Id=this_page->GetNextPageId()) {
    this_page = reinterpret_cast<TablePage *> (buffer_pool_manager_->FetchPage(page_Id));
    if (this_page->InsertTuple(row, schema_, txn, nullptr, nullptr)) {
      /*if insert successfully, we modify RowId of row
      -----in fact, the InsertTuple has done this*/
      /*fetch will pin this page, after we do update, unpin it*/
      buffer_pool_manager_->UnpinPage(page_Id, true);
      return true;
    }
    buffer_pool_manager_->UnpinPage(page_Id, false);
  }
  /*if there is no page available for inserting a new tuple*/
  if (page_Id == INVALID_PAGE_ID) {
    this_page = reinterpret_cast<TablePage *> (buffer_pool_manager_->NewPage(page_Id));
    if (this_page == nullptr) {
      LOG(WARNING) << "Fail to get a new page while inserting a tuple" << std::endl;
      return false;
    }
    /*insert the new-allocated "this page" into the linklist*/
    /*If the first_page_id is INVALID_PAGE_ID, we just make the new-allocated page the first*/
    TablePage *first_page = reinterpret_cast<TablePage *> (buffer_pool_manager_->FetchPage(first_page_id_));
    if (first_page != nullptr) {
      first_page->SetPrevPageId(page_Id);
    }
    this_page->Init(page_Id, INVALID_PAGE_ID, nullptr, nullptr);
    this_page->SetNextPageId(first_page_id_);
    buffer_pool_manager_->UnpinPage(first_page_id_, true);
    first_page_id_ = page_Id;
    if (!this_page->InsertTuple(row, schema_, txn, nullptr, nullptr)) {
      LOG(WARNING) << "Inserting a tuple to a new page fails" << std::endl;
      return false;
    }
    buffer_pool_manager_->UnpinPage(page_Id,true);
    return true;
  }
  LOG(WARNING) << "Unexpected faults in table_heap while inserting tuples" << std::endl;
  return false;
}

bool TableHeap::MarkDelete(const RowId &rid, Transaction *txn) {
  // Find the page which contains the tuple.
  auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(rid.GetPageId()));
  // If the page could not be found, then abort the transaction.
  if (page == nullptr) {
    return false;
  }
  // Otherwise, mark the tuple as deleted.
  page->WLatch();
  page->MarkDelete(rid, txn, lock_manager_, log_manager_);
  page->WUnlatch();
  buffer_pool_manager_->UnpinPage(page->GetTablePageId(), true);
  return true;
}

bool TableHeap::UpdateTuple(Row &row, RowId &rid, Transaction *txn) {
  update_faults msg = success;
  TablePage *update_page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(rid.GetPageId()));
  if (update_page == nullptr) {
    LOG(WARNING) << "Fetch page fails in UpdateTuple " << std::endl;
    return false;
  }
  /*update tuple, return message in msg*/
  Row old_row(rid);
  if (!update_page->UpdateTuple(row, &old_row, schema_, txn, nullptr, nullptr, msg)) {
    /*fetch will pin this page, after we do update, unpin it*/
    buffer_pool_manager_->UnpinPage(rid.GetPageId(), true);
    if (msg == noSpace) {
      MarkDelete(rid, txn);
      InsertTuple(row, txn);
      return true;
    } else {
      return false;
    }
  }
  /*if the update in original rid is successful, update the rid_ of row*/
  row.SetRowId(rid);
  buffer_pool_manager_->UnpinPage(rid.GetPageId(), true);
  return true;
}

void TableHeap::ApplyDelete(const RowId &rid, Transaction *txn) {
  // Step1: Find the page which contains the tuple.
  // Step2: Delete the tuple from the page.
  TablePage *delete_page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(rid.GetPageId()));
  if (delete_page == nullptr) {
    LOG(WARNING) << "Fetch page fails in ApplyDelete " << std::endl;
    return;
  }
  delete_page->ApplyDelete(rid, txn, nullptr);
  buffer_pool_manager_->UnpinPage(rid.GetPageId(), true);
}

void TableHeap::RollbackDelete(const RowId &rid, Transaction *txn) {
  // Find the page which contains the tuple.
  auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(rid.GetPageId()));
  assert(page != nullptr);
  // Rollback the delete.
  page->WLatch();
  page->RollbackDelete(rid, txn, log_manager_);
  page->WUnlatch();
  buffer_pool_manager_->UnpinPage(page->GetTablePageId(), true);
}

void TableHeap::FreeHeap() {
  /*delete all the page in buffer pool*/
  page_id_t page_id = first_page_id_;
  page_id_t next_page_id = INVALID_PAGE_ID;
  TablePage *this_page = nullptr;
  for (; page_id != INVALID_PAGE_ID; page_id = this_page->GetNextPageId()) {
    this_page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(page_id));
    next_page_id = this_page->GetNextPageId();
    buffer_pool_manager_->UnpinPage(page_id, false);
    if (!buffer_pool_manager_->DeletePage(page_id)) {
      LOG(WARNING) << "Some page in buffer pool is pinned in FreeHeap" << std::endl;
    }
    page_id = next_page_id;
  }
}

bool TableHeap::GetTuple(Row *row, Transaction *txn) {
  if (row == nullptr) {
    LOG(WARNING) << "Row is nulltpr in GetTuple" << std::endl;
    return false;
  }
  /*find the page where the row in by rowid*/
  TablePage *page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(row->GetRowId().GetPageId()));
  bool flag = page->GetTuple(row, schema_, txn, nullptr);
  buffer_pool_manager_->UnpinPage(page->GetPageId(), true);
  return flag;
}

TableIterator TableHeap::Begin(Transaction *txn) {
  TablePage *page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(first_page_id_));
  if (page == nullptr) {
    return End();
  }
  page_id_t next_id = page->GetNextPageId();
  RowId rid = INVALID_ROWID;
  /*if getfirsttuple rid fails, like: all tuples in first page are marked as deleted*/
  while (!page->GetFirstTupleRid(&rid)) {
    buffer_pool_manager_->UnpinPage(page->GetPageId(), false);
    page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(next_id));
    if (page == nullptr) {
      return End();
    }
    next_id = page->GetNextPageId();
  }
  /*after we get the rowid, we will get the tuple*/
  Row row(rid);
  if (!page->GetTuple(&row, schema_, txn, nullptr)) {
    LOG(WARNING) << "Fail to get tuple in .begin()" << std ::endl;
    return End();
  }
  return TableIterator(row, page, this, txn);
}

TableIterator TableHeap::End() { return TableIterator(Row(INVALID_ROWID), nullptr, this, nullptr); }
