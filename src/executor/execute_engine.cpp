#include "executor/execute_engine.h"
#include "glog/logging.h"
#include "storage/table_iterator.h"
#include "index/index_iterator.h"
#include <stack>
#include <math.h>
#include <algorithm>
ExecuteEngine::ExecuteEngine() {

}

dberr_t ExecuteEngine::Execute(pSyntaxNode ast, ExecuteContext *context) {
  if (ast == nullptr) {
    return DB_FAILED;
  }
  switch (ast->type_) {
    case kNodeCreateDB:
      return ExecuteCreateDatabase(ast, context);
    case kNodeDropDB:
      return ExecuteDropDatabase(ast, context);
    case kNodeShowDB:
      return ExecuteShowDatabases(ast, context);
    case kNodeUseDB:
      return ExecuteUseDatabase(ast, context);
    case kNodeShowTables:
      return ExecuteShowTables(ast, context);
    case kNodeCreateTable:
      return ExecuteCreateTable(ast, context);
    case kNodeDropTable:
      return ExecuteDropTable(ast, context);
    case kNodeShowIndexes:
      return ExecuteShowIndexes(ast, context);
    case kNodeCreateIndex:
      return ExecuteCreateIndex(ast, context);
    case kNodeDropIndex:
      return ExecuteDropIndex(ast, context);
    case kNodeSelect:
      return ExecuteSelect(ast, context);
    case kNodeInsert:
      return ExecuteInsert(ast, context);
    case kNodeDelete:
      return ExecuteDelete(ast, context);
    case kNodeUpdate:
      return ExecuteUpdate(ast, context);
    case kNodeTrxBegin:
      return ExecuteTrxBegin(ast, context);
    case kNodeTrxCommit:
      return ExecuteTrxCommit(ast, context);
    case kNodeTrxRollback:
      return ExecuteTrxRollback(ast, context);
    case kNodeExecFile:
      return ExecuteExecfile(ast, context);
    case kNodeQuit:
      return ExecuteQuit(ast, context);
    default:
      break;
  }
  return DB_FAILED;
}

dberr_t ExecuteEngine::ExecuteCreateDatabase(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteCreateDatabase" << std::endl;
#endif
  ast = ast->child_;
  DBStorageEngine *NewDBptr = new DBStorageEngine(ast->val_);
  dbs_.insert(make_pair(ast->val_, NewDBptr));
  // delete NewDBptr;
  return DB_SUCCESS;
  return DB_FAILED;
}

dberr_t ExecuteEngine::ExecuteDropDatabase(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteDropDatabase" << std::endl;
#endif
  ast = ast->child_;
  // ���ҵ�Ҫ��drop��database
  for (auto it = dbs_.begin(); it != dbs_.end(); it++) {
    if (it->first == ast->val_)  // �ҵ�
    {
      DBStorageEngine *DBToDrop = it->second;
      delete DBToDrop; // ɾ�����database
      // DBToDrop->~DBStorageEngine();
      it = dbs_.erase(it); // ��unorderedmap���Ƴ���dbs
      return DB_SUCCESS;
    }
  }
  return DB_FAILED;
}

dberr_t ExecuteEngine::ExecuteShowDatabases(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteShowDatabases" << std::endl;
#endif
  // ��ӡunordered map�е�databases
  cout << "Database:" << endl;
  if (dbs_.empty())  // ��ʱû�����ݿ�
  {
    cout << "No database."<< endl;
    return DB_FAILED;
  }
  for (auto it = dbs_.begin(); it != dbs_.end(); it++) {
    cout << it->first << endl;
  }
  return DB_SUCCESS;
}

dberr_t ExecuteEngine::ExecuteUseDatabase(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteUseDatabase" << std::endl;
#endif
  ast = ast->child_;
  // �ҵ�database��������Ϊcurrent database
  for (auto it = dbs_.begin(); it != dbs_.end(); it++) {
    if (it->first == ast->val_)  // �ҵ�
    {
      current_db_ = ast->val_; // ��Ϊcurrent database
      return DB_SUCCESS;
    }
  }
  return DB_FAILED;
}

dberr_t ExecuteEngine::ExecuteShowTables(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteShowTables" << std::endl;
#endif
  DBStorageEngine *Currentp;
  vector<TableInfo *> CurrentTable;
  std::unordered_map<std::string, DBStorageEngine *>::iterator it;
  for (it = dbs_.begin(); it != dbs_.end(); it++) {
    if (it->first == current_db_)  // �ҵ�
    {
      Currentp = it->second;
      break;
    }
  }
  if (it != dbs_.end()) {
    if (Currentp->catalog_mgr_->GetTables(CurrentTable) == DB_FAILED) return DB_FAILED;
    // ����vector�����ÿ����������
    for (auto iter = CurrentTable.begin(); iter != CurrentTable.end(); iter++) {
      cout << (*iter)->GetTableName() << endl;
    }
    return DB_SUCCESS;
  }
  return DB_FAILED;
}

dberr_t ExecuteEngine::ExecuteCreateTable(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteCreateTable" << std::endl;
#endif
  TableInfo *Newtable_info = NULL;
  Transaction *txn = NULL;
  
  ast = ast->child_;
  string NewTableName = ast->val_;
  vector<Column *> NewColumns;
  uint32_t indexnum = 0;
  bool nullable = true;
  bool uniqueable = false;
  TypeId newtype;
  /* �ҵ����ڵ�DB */
  DBStorageEngine *Currentp;
  for (auto it = dbs_.begin(); it != dbs_.end(); it++) {
    if (it->first == current_db_)  // �ҵ�
    {
      Currentp = it->second;
      break;
    }
  }
  vector<Column> primarykey;
  ast = ast->next_;  // ast type kNodeColumnDefinitionList
  if (ast->type_ == kNodeColumnDefinitionList) {
    ast = ast->child_; // ��������column��pSyntaxNode
    while (ast != NULL) {
      uniqueable = false;
      pSyntaxNode tmp = ast;
      if (ast->type_ == kNodeColumnDefinition) {
        tmp = tmp->child_;
        if (ast->val_ != NULL && strcmp(ast->val_ ,"unique")==0) uniqueable = true;
        if (tmp->type_ == kNodeIdentifier) // column name
        {
          if (strcmp(tmp->next_->val_ ,"int") == 0) {
            newtype = kTypeInt;
            // Column newcol(tmp->val_, newtype, indexnum, nullable, uniqueable);
            Column *newcolptr =
                new Column(tmp->val_, newtype, indexnum, nullable, uniqueable);
            NewColumns.push_back(newcolptr);
          } 
          else if (strcmp(tmp->next_->val_, "float")==0) {
            newtype = kTypeFloat;
            // Column newcol(tmp->val_, newtype, indexnum, nullable, uniqueable);
            Column *newcolptr = new Column(tmp->val_, newtype, indexnum, nullable, uniqueable);
            NewColumns.push_back(newcolptr);
          } 
          else if (strcmp(tmp->next_->val_, "char")==0) {
            newtype = kTypeChar;
            float l = atof(tmp->next_->child_->val_);
            // �˴�Ӧ������Լ������
            uint32_t length;
            if (ceil(l) != floor(l) || l < 0) {
              cout << "�ַ����Ȳ�������" << endl;
              return DB_FAILED;
            }
            if (l <= 0) {
              cout << "�ַ�������<=0" << endl;
              return DB_FAILED;
            }
            length = ceil(l);
            // Column newcol(tmp->val_, newtype, length, indexnum, nullable, uniqueable);
            Column *newcolptr = new Column(tmp->val_, newtype, length, indexnum, nullable, uniqueable);
            NewColumns.push_back(newcolptr);
          }
          indexnum++;
        }
      } 
      else if (ast->type_ == kNodeColumnList) {
        if (strcmp(ast->val_, "primary keys")==0) {
          tmp = ast->child_;
          while (tmp != NULL) {
            if (tmp->type_ == kNodeIdentifier) {
              for (auto i = NewColumns.begin(); i != NewColumns.end(); i++) {
                if ((*i)->GetName() == tmp->val_) {
                  Column tmpC = *i;
                  primarykey.push_back(tmpC);
                  break;
                }
              }
            }
            tmp = tmp->next_;
          }
        }
      }
      ast = ast->next_;
    }
    // TableSchema NewSchema(NewColumns);
    // TableSchema *p = new
    TableSchema* NewSchema= ALLOC_P(Currentp->catalog_mgr_->GetHeap(), TableSchema)(NewColumns);
    if (Currentp->catalog_mgr_->CreateTable(NewTableName, NewSchema, txn, Newtable_info) == DB_SUCCESS) {
      // MemHeap *heap{};
      TableInfo *currenttable{};
      // currenttable->Create(heap);

      Currentp->catalog_mgr_->GetTable(NewTableName, currenttable);
      currenttable->CreatePrimarykey(primarykey);
      if (primarykey.size() == 1) {
        primarykey[0].SetUnique();
      }
      return DB_SUCCESS;
    }
    
    return DB_FAILED;
  }
  return DB_FAILED;
}

dberr_t ExecuteEngine::ExecuteDropTable(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteDropTable" << std::endl;
#endif
  ast = ast->child_;
  DBStorageEngine *Currentp;
  std::unordered_map<std::string, DBStorageEngine *>::iterator it;
  for (it = dbs_.begin(); it != dbs_.end(); it++) {
    if (it->first == current_db_)  // �ҵ�
    {
      Currentp = it->second;
      break;
    }
  }
  if (it != dbs_.end()) {
    return (Currentp->catalog_mgr_->DropTable(ast->val_));
  }
  return DB_FAILED;
}

dberr_t ExecuteEngine::ExecuteShowIndexes(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteShowIndexes" << std::endl;
#endif
  DBStorageEngine *Currentp;
  vector<TableInfo*> tables;
  for (auto it = dbs_.begin(); it != dbs_.end(); it++) {
    if (it->first == current_db_) 
    {
      Currentp = it->second;
      break;
    }
  }
  Currentp->catalog_mgr_->GetTables(tables); // �Ѹ�db�е�table����tables
  if (ast->type_ == kNodeShowIndexes) {
    for (auto iter = tables.begin(); iter != tables.end(); iter++) {
      string tablename = (*iter)->GetTableName();
      vector<IndexInfo *> indexes_;
      Currentp->catalog_mgr_->GetTableIndexes(tablename, indexes_);
      // show indexes
      cout << "Table"
           << "Non_unique"
           << "Key_name"
           << "Column name"
           << "Index_type" << endl;
      for (auto it = indexes_.begin(); it != indexes_.end(); it++) {
        cout << tablename << " ";
        IndexSchema *indexc = (*it)->GetIndexKeySchema();
        vector<Column*> indexcolumns = indexc->GetColumns();
        for (auto i = indexcolumns.begin(); i != indexcolumns.end(); i++) {
          cout << (*i)->GetName() << " ";
        }
        cout << endl;
      }
    }
    return DB_SUCCESS;
  }
  return DB_FAILED;
}

dberr_t ExecuteEngine::ExecuteCreateIndex(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteCreateIndex" << std::endl;
#endif
  DBStorageEngine *Currentp;
  Transaction *txn = NULL;
  string indexname;
  string tablename;
  string method;
  vector<string> indexkeys;
  IndexInfo *index_info = NULL;
  for (auto it = dbs_.begin(); it != dbs_.end(); it++) {
    if (it->first == current_db_)
    {
      Currentp = it->second;
      break;
    }
  }
  ast = ast->child_;
  indexname = ast->val_;
  ast = ast->next_;
  tablename = ast->val_;
  ast = ast->next_;
  if (ast->type_ == kNodeColumnList) {
    pSyntaxNode tmp = ast->child_;
    while (tmp != NULL) {
      indexkeys.push_back(tmp->val_);
      tmp = tmp->next_;
    }
  }

  return Currentp->catalog_mgr_->CreateIndex(tablename, indexname, indexkeys, txn, index_info);
  
  return DB_FAILED;
}

dberr_t ExecuteEngine::ExecuteDropIndex(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteDropIndex" << std::endl;
#endif
  DBStorageEngine *Currentp;
  for (auto it = dbs_.begin(); it != dbs_.end(); it++) {
    if (it->first == current_db_)
    {
      Currentp = it->second;
      break;
    }
  }
  ast = ast->child_;
  vector<TableInfo *> tables;
  Currentp->catalog_mgr_->GetTables(tables);
  for (auto iter = tables.begin(); iter != tables.end(); iter++) {
    vector<IndexInfo *> indexes;
    Currentp->catalog_mgr_->GetTableIndexes((*iter)->GetTableName(), indexes);
    for (auto i = indexes.begin(); i != indexes.end(); i++) {
      if ((*i)->GetIndexName() == ast->val_)  // find the index
      {
        Currentp->catalog_mgr_->DropIndex((*iter)->GetTableName(), ast->val_);
        return DB_SUCCESS;
      }
    }
  }
  return DB_INDEX_NOT_FOUND;
  
  return DB_FAILED;
}

dberr_t ExecuteEngine::NewTravel(DBStorageEngine *Currentp, TableInfo *currenttable,
                                        pSyntaxNode root, vector<RowId> * result) {
    Transaction *txn = NULL;
  if (root->type_ == kNodeConnector) {
    vector<RowId> left, right;
    NewTravel(Currentp, currenttable, root->child_, &left);
    NewTravel(Currentp, currenttable, root->child_->next_, &right);
    if (strcmp(root->val_, "and") == 0) {
      for (auto iterleft = left.begin(); iterleft != left.end(); iterleft++) {
        if (find(right.begin(), right.end(), *iterleft) != right.end()) {
          (*result).push_back(*iterleft);
        }
      }
    } 
    else if (strcmp(root->val_, "or") == 0) {
      (*result).insert((*result).end(), left.begin(), left.end());
      (*result).insert((*result).end(), right.begin(), right.end());
      sort((*result).begin(), (*result).end());
      (*result).erase(unique((*result).begin(), (*result).end()), (*result).end());
    }
    if (!result->empty())
        return DB_SUCCESS;
    return DB_FAILED;
  } 
  else if (root->type_ == kNodeCompareOperator) {
    char *cmpoperator = root->val_;
    char *op1 = root->child_->val_;
    char *op2 = root->child_->next_->val_;
    // if key ����index
    MemHeap *heap{};
    IndexInfo *nowindex = IndexInfo::Create(heap);
    // ���ڸ��е�����
    if (Currentp->catalog_mgr_->GetIndex(currenttable->GetTableName(), op1, nowindex) != DB_INDEX_NOT_FOUND) {
      vector<RowId> result;
      if (root->child_->next_->type_ == kNodeNumber || root->child_->next_->type_ == kNodeString) {
        uint32_t op1index;
          currenttable->GetSchema()->GetColumnIndex(op1, op1index);
          TypeId typeop1 = currenttable->GetSchema()->GetColumn(op1index)->GetType();
          vector<Field> keyrowfield;
          if (typeop1 == kTypeInt) {
            keyrowfield.push_back(Field(typeop1, atoi(op2)));
          }
          else if (typeop1 == kTypeFloat) {
            keyrowfield.push_back(Field(typeop1, (float)atof(op2)));
          } 
          else if (typeop1 == kTypeChar) {
            keyrowfield.push_back(Field(typeop1, op2, strlen(op2), true));
          }
          Row keyrow(keyrowfield);
          vector<RowId> scanresult;
          int position{};
          page_id_t leaf_page_id{};
          nowindex->GetIndex()->ScanKey(keyrow, scanresult, position, leaf_page_id, txn);
          BufferPoolManager *buffer_pool_manager = NULL;
          IndexIterator<GenericKey<32>, RowId, GenericComparator<32>> indexiter(leaf_page_id, position,
                                                                              buffer_pool_manager);
          /* nowindex->Create()
          for (indexiter; indexiter != ;indexiter++) {
              (*indexiter).
          }*/
      } 
      else if (root->child_->next_->type_ == kNodeNull) {
        if (strcmp(cmpoperator, "is") == 0) {


        } else if (strcmp(cmpoperator, "not") == 0) {
        }
      }
    } 
    // �����ڸ��е�����
    else {
      TableIterator tableit(currenttable->GetTableHeap()->Begin(txn));
      for (tableit == currenttable->GetTableHeap()->Begin(txn); tableit != currenttable->GetTableHeap()->End();
           tableit++) {
        if (TravelWithoutIndex(currenttable, tableit, root) == kTrue) {
          (*result).push_back((*tableit).GetRowId());
        }
      }
      if (!(*result).empty())
          return DB_SUCCESS;
      return DB_FAILED;
    }
  }
  return DB_FAILED;
}

CmpBool ExecuteEngine::TravelWithoutIndex(TableInfo *currenttable, TableIterator &tableit, pSyntaxNode root) {
  
    char *cmpoperator = root->val_;
    char *op1 = root->child_->val_;
    if (root->child_->next_->type_ == kNodeNumber || root->child_->next_->type_ == kNodeString) {
      char *op2 = root->child_->next_->val_;
      Field *now{};
      uint32_t op1index{};
      if (currenttable->GetSchema()->GetColumnIndex(op1, op1index)) {
        now = (*tableit).GetField(op1index);
        TypeId typeop1 = currenttable->GetSchema()->GetColumn(op1index)->GetType();
        Field *pto{};
        if (typeop1 == kTypeInt) {
          pto = new Field(typeop1, atoi(op2));
        } else if (typeop1 == kTypeFloat) {
          pto = new Field(typeop1, (float)atof(op2));
        } else if (typeop1 == kTypeChar) {
          pto = new Field(typeop1, op2, strlen(op2), true);
        }

        if (strcmp(cmpoperator, "=") == 0) {
          CmpBool returnvalue = now->CompareEquals(*pto);
          delete pto;
          return returnvalue;
        } 
        else if (strcmp(cmpoperator, ">") == 0) {
          CmpBool returnvalue = now->CompareGreaterThan(*pto);
          delete pto;
          return returnvalue;
        } 
        else if (strcmp(cmpoperator, "<") == 0) {
          CmpBool returnvalue = now->CompareLessThan(*pto);
          delete pto;
          return returnvalue;
        } 
        else if (strcmp(cmpoperator, "!=") == 0) {
          CmpBool returnvalue = now->CompareNotEquals(*pto);
          delete pto;
          return returnvalue;
        } 
        else if (strcmp(cmpoperator, "<=") == 0) {
          CmpBool returnvalue = now->CompareLessThanEquals(*pto);
          delete pto;
          return returnvalue;
        } 
        else if (strcmp(cmpoperator, ">=") == 0) {
          CmpBool returnvalue = now->CompareGreaterThanEquals(*pto);
          delete pto;
          return returnvalue;
        }
      }
    } else if (root->child_->next_->type_ == kNodeNull) {
      if (strcmp(cmpoperator, "is") == 0) {
        // is null
        Field *now{};
        uint32_t op1index{};
        if (currenttable->GetSchema()->GetColumnIndex(op1, op1index)) {
          now = (*tableit).GetField(op1index);
          return GetCmpBool(now->IsNull());
        }
      } else if (strcmp(cmpoperator, "not") == 0) {
        // not null
        Field *now{};
        uint32_t op1index{};
        if (currenttable->GetSchema()->GetColumnIndex(op1, op1index)) {
          now = (*tableit).GetField(op1index);
          return GetCmpBool(!(now->IsNull()));
        }
      }
    }
    return kFalse;
}

CmpBool ExecuteEngine::Travel(TableInfo *currenttable, TableIterator &tableit, pSyntaxNode root) {
  if (root->type_ == kNodeConnector) {
    if (strcmp(root->val_, "and") == 0) {
      if (Travel(currenttable, tableit, root->child_) == kTrue &&
          Travel(currenttable, tableit, root->child_->next_) == kTrue)
        return kTrue;
      return kFalse;
    } 
    else if (strcmp(root->val_, "or") == 0) {
      if (Travel(currenttable, tableit, root->child_) == kTrue ||
          Travel(currenttable, tableit, root->child_->next_) == kTrue)
        return kTrue;
      return kFalse;
    }
    return kFalse; // ���ܻ��б��Connector�ɲ�����������߷���һ��
  } 
  else if (root->type_ == kNodeCompareOperator) {
    char *cmpoperator = root->val_;
    char *op1 = root->child_->val_;
    if (root->child_->next_->type_ == kNodeNumber || root->child_->next_->type_ == kNodeString) {
      char *op2 = root->child_->next_->val_;
      Field *now{};
      uint32_t op1index{};
      if (currenttable->GetSchema()->GetColumnIndex(op1, op1index)) {
        now = (*tableit).GetField(op1index);
        TypeId typeop1 = currenttable->GetSchema()->GetColumn(op1index)->GetType();
        Field *pto;
        if (typeop1 == kTypeInt) {
          Field o(typeop1, atoi(op2));
          pto = &o;
        } 
        else if (typeop1 == kTypeFloat) {
          Field o(typeop1, (float)atof(op2));
          pto = &o;
        } 
        else if (typeop1 == kTypeChar) {
          Field o(typeop1, op2, strlen(op2), true);
          pto = &o;
        }
        
        if (strcmp(cmpoperator, "=") == 0) {
          return now->CompareEquals(*pto);
        } 
        else if (strcmp(cmpoperator, ">") == 0) {
          return now->CompareGreaterThan(*pto);
        } 
        else if (strcmp(cmpoperator, "<") == 0) {
          return now->CompareLessThan(*pto);
        } 
        else if (strcmp(cmpoperator, "!=") == 0) {
          return now->CompareNotEquals(*pto);
        } 
        else if (strcmp(cmpoperator, "<=") == 0) {
          return now->CompareLessThanEquals(*pto);
        } 
        else if (strcmp(cmpoperator, ">=") == 0) {
          return now->CompareGreaterThanEquals(*pto);
        }
      }
    } 
    else if (root->child_->next_->type_ == kNodeNull) {
      if (strcmp(cmpoperator, "is") == 0) {
        // is null
        Field *now{};
        uint32_t op1index{};
        if (currenttable->GetSchema()->GetColumnIndex(op1, op1index)) {
          now = (*tableit).GetField(op1index);
          return GetCmpBool(now->IsNull());
        }
      } 
      else if (strcmp(cmpoperator, "not") == 0) {
        // not null
        Field *now{};
        uint32_t op1index{};
        if (currenttable->GetSchema()->GetColumnIndex(op1, op1index)) {
          now = (*tableit).GetField(op1index);
          return GetCmpBool(!(now->IsNull()));
        }
      }
    }

    
  }
  return kFalse;
}

dberr_t ExecuteEngine::ExecuteSelect(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteSelect" << std::endl;
#endif
  DBStorageEngine *Currentp;
  ast = ast->child_;
  pSyntaxNode tmp = ast;
  ast = ast->next_;
  TableInfo *currenttable;
  vector<Column *> columns;
  Transaction *txn{};
  for (auto it = dbs_.begin(); it != dbs_.end(); it++) {
    if (it->first == current_db_)
    {
      Currentp = it->second;
      break;
    }
  }
  if(Currentp->catalog_mgr_->GetTable(ast->val_, currenttable) == DB_TABLE_NOT_EXIST)
      return DB_TABLE_NOT_EXIST;
  Schema *columnToSelect = currenttable->GetSchema();
  /* ��Ҫ�ҵ�column�ŵ�columns�� */
  if (tmp->type_ == kNodeAllColumns) {
    columns = columnToSelect->GetColumns();
  } 
  else if (tmp->type_ == kNodeColumnList) {
    tmp = tmp->child_;
    while (tmp) {
      uint32_t columnindex = 0;
      if (columnToSelect->GetColumnIndex(tmp->val_, columnindex) == DB_COLUMN_NAME_NOT_EXIST)
        return DB_COLUMN_NAME_NOT_EXIST;
      Column nowColumn = columnToSelect->GetColumn(columnToSelect->GetColumnIndex(tmp->val_, columnindex));
      columns.push_back(&nowColumn);
      tmp = tmp->next_;
    }
  }
  ast = ast->next_;
  // �˴���ʼ�ж�����
  if (ast->type_ == kNodeConditions) {
    pSyntaxNode root= ast->child_;
    vector<RowId> result;
    
    if (NewTravel(Currentp, currenttable, root, &result) == DB_SUCCESS) {
      for (auto i = result.begin(); i != result.end(); i++) {
        Row nowrow(*i);
        currenttable->GetTableHeap()->GetTuple(&nowrow, txn);
        for (auto fielditer = columns.begin(); fielditer != columns.end(); fielditer++) {
          uint32_t fieldid;
          currenttable->GetSchema()->GetColumnIndex((*fielditer)->GetName(), fieldid);
          cout << nowrow.GetField(fieldid)->GetData() << " ";
        }
        cout << endl;
      }
    }
    // ͨ����������ѯ
    /* TableIterator tableit(currenttable->GetTableHeap()->Begin(txn));
    for (tableit == currenttable->GetTableHeap()->Begin(txn); tableit != currenttable->GetTableHeap()->End();
         tableit++) {
      if (Travel(currenttable, tableit, root) == kTrue) {
          // ��ӡ
        Row row((*tableit).GetRowId());
        currenttable->GetTableHeap()->GetTuple(&row, txn);
        
        for (auto i = columns.begin(); i != columns.end(); i++) {
          uint32_t tmpcolumnindex{};
          currenttable->GetSchema()->GetColumnIndex((*i)->GetName(), tmpcolumnindex);
          Field* tmpField = row.GetField(tmpcolumnindex);
          cout << tmpField->GetData() << " ";
        }
        // row.GetField();
        cout << "-------------" << endl;
        
      }
    }*/
    return DB_SUCCESS;
  } 
  else if (ast == NULL) { // û������
    TableIterator tableit(currenttable->GetTableHeap()->Begin(txn));
    for (tableit == currenttable->GetTableHeap()->Begin(txn); tableit != currenttable->GetTableHeap()->End();
         tableit++) {
      // ��ӡ
      Row row((*tableit).GetRowId());
      currenttable->GetTableHeap()->GetTuple(&row, txn);

      for (auto i = columns.begin(); i != columns.end(); i++) {
        uint32_t tmpcolumnindex{};
        currenttable->GetSchema()->GetColumnIndex((*i)->GetName(), tmpcolumnindex);
        Field *tmpField = row.GetField(tmpcolumnindex);
        cout << tmpField->GetData() << " ";
      }
      // row.GetField();
      cout << "-------------" << endl;
      
    }
    return DB_SUCCESS;
  }
  return DB_FAILED;
}

dberr_t ExecuteEngine::ExecuteInsert(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteInsert" << std::endl;
#endif
  DBStorageEngine *Currentp;
  TableInfo *currenttable;
  Transaction *txn{};
  vector<IndexInfo *> indexes;
  /* �ҵ���ǰdb */
  for (auto it = dbs_.begin(); it != dbs_.end(); it++) {
    if (it->first == current_db_)  // �ҵ�
    {
      Currentp = it->second;
      break;
    }
  }
  ast = ast->child_;
  /* �ҵ�Ҫ������ı� */
  if (ast->type_ == kNodeIdentifier) {
    if (Currentp->catalog_mgr_->GetTable(ast->val_, currenttable) == DB_TABLE_NOT_EXIST) 
        return DB_TABLE_NOT_EXIST;
  }
  ast = ast->next_; // kColumnValues
  ast = ast->child_;
  vector<Field> newfield;
  while (ast != NULL) {
    //Field tmpField()
    if (ast->type_ == kNodeNumber) {
      if (strchr(ast->val_, '.') == NULL)  // float
      {
        Field tmpField(kTypeFloat, (float)atof(ast->val_));
        newfield.push_back(tmpField);
      } 
      else { // integer
        Field tmpField(kTypeInt, atoi(ast->val_));
        newfield.push_back(tmpField);
      }
    } 
    else if (ast->type_ == kNodeString) {
      Field tmpField(kTypeChar, ast->val_, strlen(ast->val_), true);
      newfield.push_back(tmpField);
    } 
    else if (ast->type_ == kNodeNull) {
      TypeId tmptype = currenttable->GetSchema()->GetColumn(ast->id_ - 1)->GetType();
      if (currenttable->GetSchema()->GetColumn(ast->id_ - 1)->IsNullable() == false) {
        cout << "������Ϊ��" << endl;
        return DB_FAILED;
      }
      Field tmpField(tmptype);
      newfield.push_back(tmpField);
    }
    ast = ast->next_;
  }
  Row row(newfield);
  vector<Column *> columns = currenttable->GetSchema()->GetColumns();
  Currentp->catalog_mgr_->GetTableIndexes(currenttable->GetTableName(), indexes);
  // ���newfield�Ƿ���ϲ�������
  // ���unique
  vector<Column*> uniqueColumns;
  for (auto columnsiter = columns.begin(); columnsiter != columns.end(); columnsiter++) {
    if ((*columnsiter)->IsUnique()) {
      // �����������index
      for (auto iterindexes = indexes.begin(); iterindexes != indexes.end(); iterindexes++) {
        if ((*iterindexes)->GetIndexName() == (*columnsiter)->GetName()) {
            // ͨ��index�������ظ�
          vector<RowId> result;
          int position;
          page_id_t leaf_page_id;
          if((*iterindexes)->GetIndex()->ScanKey(row, result, position, leaf_page_id, txn) == DB_SUCCESS) {
            cout << "����Unique�У���Ӧ�ò����ظ���Ԫ��" << endl;
            return DB_FAILED;
          }
        }
      }
      // ���������û��index����tableiterator����������ظ�tuple
      TableIterator tableit(currenttable->GetTableHeap()->Begin(txn));
      for (tableit == currenttable->GetTableHeap()->Begin(txn); tableit != currenttable->GetTableHeap()->End();
           tableit++) {
        uint32_t indexop1{};
        currenttable->GetSchema()->GetColumnIndex((*columnsiter)->GetName(), indexop1);
        Field * currentfield = (*tableit).GetField(indexop1);
        if (currentfield->CompareEquals(newfield[indexop1]) == kTrue)
            return DB_FAILED;
      }
    }
  }
  vector<Column> primarykey = currenttable->GetPrimarykey();
  // ��������� ���������ظ�Ԫ��
  if (primarykey.size() > 1) {
    vector<uint32_t> columnindexes;
    vector<uint32_t>::iterator iter;
    for (auto piter = primarykey.begin(); piter != primarykey.end(); piter++) {
      uint32_t tmpindex;
      currenttable->GetSchema()->GetColumnIndex((*piter).GetName(), tmpindex);
      columnindexes.push_back(tmpindex);
    }
      // ��ʱ˵������������
    TableIterator tableit(currenttable->GetTableHeap()->Begin(txn));
    for (tableit == currenttable->GetTableHeap()->Begin(txn); tableit != currenttable->GetTableHeap()->End();
         tableit++) {
      for (iter = columnindexes.begin(); iter != columnindexes.end(); iter++) {
        if (row.GetField(*iter)->CompareEquals(*((*tableit).GetField(*iter))) != kTrue) {
          break;
        }
      }
      if (iter == columnindexes.end())  // ���е�field��һ��
      {
        return DB_FAILED;
      }
    }
  }
  
  if (currenttable->GetTableHeap()->InsertTuple(row, txn)) {
      // ���indexex
     for (auto iterindexes = indexes.begin(); iterindexes != indexes.end(); iterindexes++) {
       if((*iterindexes)->GetIndex()->InsertEntry(row, row.GetRowId(), txn) == DB_FAILED)
          return DB_FAILED;
     }
    return DB_SUCCESS;
  }
  return DB_FAILED;
}

dberr_t ExecuteEngine::ExecuteDelete(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteDelete" << std::endl;
#endif
  DBStorageEngine *Currentp;
  TableInfo *currenttable;
  Transaction *txn{};
  /* �ҵ���ǰdb */
  for (auto it = dbs_.begin(); it != dbs_.end(); it++) {
    if (it->first == current_db_)  // �ҵ�
    {
      Currentp = it->second;
      break;
    }
  }
  ast = ast->child_;
  /* �ҵ�Ҫ��ɾ���ı� */
  if (ast->type_ == kNodeIdentifier) {
    if (Currentp->catalog_mgr_->GetTable(ast->val_, currenttable) == DB_TABLE_NOT_EXIST) return DB_TABLE_NOT_EXIST;
  }
  ast = ast->next_; // kNodeConditions
  vector<IndexInfo *> indexes;
  Currentp->catalog_mgr_->GetTableIndexes(currenttable->GetTableName(), indexes);
  if (ast->type_ == kNodeConditions) {
    pSyntaxNode root = ast->child_;
    TableIterator tableit(currenttable->GetTableHeap()->Begin(txn));
    for (tableit == currenttable->GetTableHeap()->Begin(txn); tableit != currenttable->GetTableHeap()->End();
         tableit++) {
      if (Travel(currenttable, tableit, root) == kTrue) {
        // ɾ��
        Row row((*tableit).GetRowId());
        if(currenttable->GetTableHeap()->MarkDelete((*tableit).GetRowId(), txn) == false)
            return DB_FAILED;
        // ��index��ɾ��
        currenttable->GetTableHeap()->ApplyDelete((*tableit).GetRowId(), txn);
        for (auto iterindexes = indexes.begin(); iterindexes != indexes.end(); iterindexes++) {
          if ((*iterindexes)->GetIndex()->RemoveEntry(row, row.GetRowId(), txn) == DB_FAILED) return DB_FAILED;
        }
        return DB_SUCCESS;
      }
    }
  }
  return DB_FAILED;
}

dberr_t ExecuteEngine::ExecuteUpdate(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteUpdate" << std::endl;
#endif
  DBStorageEngine *Currentp;
  TableInfo *currenttable;
  Transaction *txn{};
  /* �ҵ���ǰdb */
  for (auto it = dbs_.begin(); it != dbs_.end(); it++) {
    if (it->first == current_db_)  // �ҵ�
    {
      Currentp = it->second;
      break;
    }
  }
  ast = ast->child_;
  /* �ҵ�Ҫ�����µı� */
  if (ast->type_ == kNodeIdentifier) {
    if (Currentp->catalog_mgr_->GetTable(ast->val_, currenttable) == DB_TABLE_NOT_EXIST) return DB_TABLE_NOT_EXIST;
  }
  vector<Field> update;
  vector<uint32_t> FieldColumn;
  int updatecolumn = 0;
  ast = ast->next_; // kNodeUpdateValues
  if (ast->type_ == kNodeUpdateValues) {
    ast = ast->child_;
    while (ast != NULL) {
      char *op1 = ast->child_->val_;
      uint32_t op1index;
      if(currenttable->GetSchema()->GetColumnIndex(op1, op1index) == DB_COLUMN_NAME_NOT_EXIST)
          return DB_COLUMN_NAME_NOT_EXIST;
      FieldColumn.push_back(op1index);
      char *op2 = ast->child_->next_->val_;
      if (ast->child_->next_->type_ == kNodeNumber) {
        if (strchr(op2, '.') == NULL)  // float
        {
          Field tmpField(kTypeFloat, (float)atof(op2));
          update.push_back(tmpField);
        } 
        else {  // integer
          Field tmpField(kTypeInt, atoi(op2));
          update.push_back(tmpField);
        }
      } 
      else if (ast->child_->next_->type_ == kNodeString) {
        Field tmpField(kTypeChar, op2, strlen(op2), true);
        update.push_back(tmpField);
      } 
      else if (ast->child_->next_->type_ == kNodeNull) {
        TypeId tmptype = currenttable->GetSchema()->GetColumn(ast->child_->next_->id_ - 1)->GetType();
        if (currenttable->GetSchema()->GetColumn(ast->child_->next_->id_ - 1)->IsNullable() == false) {
          cout << "������Ϊ��" << endl;
          return DB_FAILED;
        }
        Field tmpField(tmptype);
        update.push_back(tmpField);
      }
      ast = ast->next_;
    }
  }
  pSyntaxNode astCondition = ast->next_;  // kNodeConditions
  if (astCondition->type_ == kNodeConditions) {
    pSyntaxNode root = ast->child_;
    TableIterator tableit(currenttable->GetTableHeap()->Begin(txn));
    // update֮��Ҫupdateindexes
    vector<IndexInfo *> indexes;
    for (tableit == currenttable->GetTableHeap()->Begin(txn); tableit != currenttable->GetTableHeap()->End();
         tableit++) {
      if (Travel(currenttable, tableit, root) == kTrue) {
          // update
          Row row1((*tableit).GetRowId());
          
          currenttable->GetTableHeap()->GetTuple(&row1, txn);
          Row row = row1;
          vector<Field*> pre = row.GetFields();
          for (auto updateiter = update.begin(); updateiter != update.end(); updateiter++) {
            Swap(*(pre[updatecolumn]), (*updateiter));
            updatecolumn++;
          }
          RowId n = (*tableit).GetRowId();
          if(currenttable->GetTableHeap()->UpdateTuple(row, n, txn) == false) return DB_FAILED;
          
          // ��indexes
          if (Currentp->catalog_mgr_->GetTableIndexes(currenttable->GetTableName(), indexes) != DB_INDEX_NOT_FOUND) {
            for (auto iterindexes = indexes.begin(); iterindexes != indexes.end(); iterindexes++) {
              // if ((*iterindexes)->GetIndex()->InsertEntry(row, row.GetRowId(), txn) == DB_FAILED) return DB_FAILED;
              if ((*iterindexes)->GetIndex()->RemoveEntry(row1, row1.GetRowId(), txn) == DB_FAILED) 
                  return DB_FAILED;
              if ((*iterindexes)->GetIndex()->InsertEntry(row, row.GetRowId(), txn) == DB_FAILED)
                  return DB_FAILED;
            }
          }
      }
    }
    return DB_SUCCESS;
  }
  return DB_FAILED;
}

dberr_t ExecuteEngine::ExecuteTrxBegin(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteTrxBegin" << std::endl;
#endif
  return DB_FAILED;
}

dberr_t ExecuteEngine::ExecuteTrxCommit(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteTrxCommit" << std::endl;
#endif
  return DB_FAILED;
}

dberr_t ExecuteEngine::ExecuteTrxRollback(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteTrxRollback" << std::endl;
#endif
  return DB_FAILED;
}

dberr_t ExecuteEngine::ExecuteExecfile(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteExecfile" << std::endl;
#endif
  fstream sqlfile;
  char *filename = ast->child_->val_;
  sqlfile.open(filename);
  if (!sqlfile.is_open())
    return DB_FAILED;
  else {
    // fgets()
    /* while (!sqlfile.end()) {
      char *input{};
      char ch{};
      int i = 0;
      while ((ch = sqlfile.get()) != ';') {
        input[i++] = ch;
      }
      input[i] = ch;  // ;
      getchar();
      InputCommand(input, buf_size);
    }*/
  }
  return DB_FAILED;
}

dberr_t ExecuteEngine::ExecuteQuit(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteQuit" << std::endl;
#endif
  ASSERT(ast->type_ == kNodeQuit, "Unexpected node type.");
  context->flag_quit_ = true;
  return DB_SUCCESS;
}
