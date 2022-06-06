#ifndef MINISQL_TABLE_ITERATOR_H
#define MINISQL_TABLE_ITERATOR_H

#include "common/rowid.h"
#include "record/row.h"
#include "transaction/transaction.h"


class TableHeap;
class TablePage;
class TableIterator {

public:
  // you may define your own constructor based on your member variables
  //explicit TableIterator();
  TableIterator() = delete;
  
  explicit TableIterator(Row row, TablePage *table_page, TableHeap *table_heap, Transaction *txn);

  explicit TableIterator(const TableIterator &other);
  
  ~TableIterator();

  inline bool operator==(const TableIterator &itr) const { return row_.GetRowId() == itr.row_.GetRowId(); }

  inline bool operator!=(const TableIterator &itr) const { return !(itr == (*this)); }

  const Row &operator*();

  Row *operator->();

  TableIterator &operator++();

  TableIterator operator++(int);

private:
  // add your own private member variables here
  Row row_;
  TablePage *cur_page_;
  TableHeap *table_heap_;
  Transaction *txn_;
};

#endif //MINISQL_TABLE_ITERATOR_H
