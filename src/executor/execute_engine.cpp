#include "executor/execute_engine.h"
#include "glog/logging.h"
#include <stack>
#include <math.h>
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
  DBStorageEngine NewDB(ast->val_);
  DBStorageEngine *NewDBptr = &NewDB;
  dbs_.insert(make_pair(ast->val_, NewDBptr));
  return DB_SUCCESS;
  return DB_FAILED;
}

dberr_t ExecuteEngine::ExecuteDropDatabase(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteDropDatabase" << std::endl;
#endif
  ast = ast->child_;
  // 先找到要被drop的database
  std::unordered_map<std::string, DBStorageEngine *>::iterator it;
  for (it = dbs_.begin(); it != dbs_.end(); it++) {
    if (it->first == ast->val_)  // 找到
    {
      DBStorageEngine *DBToDrop = it->second;
      delete DBToDrop; // 删除这个database
      return DB_SUCCESS;
    }
  }
  return DB_FAILED;
}

dberr_t ExecuteEngine::ExecuteShowDatabases(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteShowDatabases" << std::endl;
#endif
  // 打印unordered map中的databases
  cout << "Database:" << endl;
  std::unordered_map<std::string, DBStorageEngine *>::iterator it;
  if (dbs_.begin() == dbs_.end()) // 此时没有数据库
    return DB_FAILED;
  for (it = dbs_.begin(); it != dbs_.end(); it++) {
    cout << it->first << endl;
  }
  return DB_SUCCESS;
}

dberr_t ExecuteEngine::ExecuteUseDatabase(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteUseDatabase" << std::endl;
#endif
  ast = ast->child_;
  // 找到database并把它作为current database
  std::unordered_map<std::string, DBStorageEngine *>::iterator it;
  for (it = dbs_.begin(); it != dbs_.end(); it++) {
    if (it->first == ast->val_)  // 找到
    {
      current_db_ = ast->val_; // 作为current database
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
    if (it->first == current_db_)  // 找到
    {
      Currentp = it->second;
      break;
    }
  }
  if (it != dbs_.end()) {
    Currentp->catalog_mgr_->GetTables(CurrentTable);
    // 遍历vector，输出每个表的名字
    vector<TableInfo *>::iterator iter;
    for (iter = CurrentTable.begin(); iter != CurrentTable.end(); iter++) {
      cout << (*iter)->GetTableName() << endl;
    }
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
  vector<Column *>::iterator i;
  uint32_t indexnum = 0;
  bool nullable = true;
  bool uniqueable = false;
  TypeId newtype;
  /* 找到现在的DB */
  DBStorageEngine *Currentp;
  std::unordered_map<std::string, DBStorageEngine *>::iterator it;
  for (it = dbs_.begin(); it != dbs_.end(); it++) {
    if (it->first == current_db_)  // 找到
    {
      Currentp = it->second;
      break;
    }
  }
  ast = ast->child_;  // ast type kNodeColumnDefinitionList
  if (ast->type_ == kNodeColumnDefinitionList) {
    ast = ast->child_; // 遍历生成column的pSyntaxNode
    while (ast != NULL) {
      pSyntaxNode tmp = ast;
      if (ast->type_ == kNodeColumnDefinition) {
        tmp = tmp->child_;
        if (strcmp(ast->val_ ,"unique")==0) uniqueable = true;
        if (tmp->type_ == kNodeIdentifier) // column name
        {
          if (strcmp(tmp->next_->val_ ,"int") == 0) {
            newtype = kTypeInt;
            Column newcol(tmp->val_, newtype, indexnum, nullable, uniqueable);
            NewColumns.push_back(&newcol);
          } 
          else if (strcmp(tmp->next_->val_, "float")==0) {
            newtype = kTypeFloat;
            Column newcol(tmp->val_, newtype, indexnum, nullable, uniqueable);
            NewColumns.push_back(&newcol);
          } 
          else if (strcmp(tmp->next_->val_, "char")==0) {
            newtype = kTypeChar;
            float l = atof(tmp->next_->child_->val_);
            // 此处应该增加约束条件
            uint32_t length;
            if (ceil(l) != floor(l) || l < 0) {
              cout << "字符长度不是整数" << endl;
              return DB_FAILED;
            }
            length = ceil(l);
            Column newcol(tmp->val_, newtype, length, indexnum, nullable, uniqueable);
            NewColumns.push_back(&newcol);
          }
          indexnum++;
        }
      } 
      else if (ast->type_ == kNodeColumnList) {
        if (strcmp(ast->val_, "primary key")==0) {
          vector<Column *> primarykey;
          tmp = ast->child_;
          while (tmp != NULL) {
            if (tmp->type_ == kNodeIdentifier) {
              for (i = NewColumns.begin(); i != NewColumns.end(); i++) {
                if ((*i)->GetName() == tmp->val_) {
                  Column tmpC = *i;
                  primarykey.push_back(&tmpC);
                }
              }
            }
            tmp = tmp->next_;
          }
          // txn->SetPrimaryKey(primarykey);
        }
      }
      ast = ast->next_;
    }
    TableSchema NewSchema(NewColumns);
    Currentp->catalog_mgr_->CreateTable(NewTableName, &NewSchema, txn, Newtable_info);
    return DB_SUCCESS;
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
    if (it->first == current_db_)  // 找到
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
  vector<TableInfo *>::iterator iter;
  std::unordered_map<std::string, DBStorageEngine *>::iterator it;
  for (it = dbs_.begin(); it != dbs_.end(); it++) {
    if (it->first == current_db_) 
    {
      Currentp = it->second;
      break;
    }
  }
  Currentp->catalog_mgr_->GetTables(tables); // 把该db中的table放入tables
  if (ast->type_ == kNodeShowIndexes) {
    for (iter = tables.begin(); iter != tables.end(); iter++) {
      string tablename = (*iter)->GetTableName();
      vector<IndexInfo *> indexes_;
      vector<IndexInfo *>::iterator it;
      Currentp->catalog_mgr_->GetTableIndexes(tablename, indexes_);
      // show indexes
      cout << "Table"
           << "Non_unique"
           << "Key_name"
           << "Column name"
           << "Index_type" << endl;
      for (it = indexes_.begin(); it != indexes_.end(); it++) {
        cout << tablename << " ";
        IndexSchema *indexc = (*it)->GetIndexKeySchema();
        vector<Column*> indexcolumns = indexc->GetColumns();
        vector<Column *>::iterator i;
        for (i = indexcolumns.begin(); i != indexcolumns.end(); i++) {
          cout << (*i)->GetName() << " ";
        }
        cout << endl;
      }
    }
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
  std::unordered_map<std::string, DBStorageEngine *>::iterator it;
  for (it = dbs_.begin(); it != dbs_.end(); it++) {
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
  // use method
  if (ast->next_ != NULL) {
    ast = ast->next_;
    method = ast->val_;
  }

  Currentp->catalog_mgr_->CreateIndex(tablename, indexname, indexkeys, txn, index_info);
  
  return DB_FAILED;
}

dberr_t ExecuteEngine::ExecuteDropIndex(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteDropIndex" << std::endl;
#endif
  DBStorageEngine *Currentp;
  std::unordered_map<std::string, DBStorageEngine *>::iterator it;
  for (it = dbs_.begin(); it != dbs_.end(); it++) {
    if (it->first == current_db_)
    {
      Currentp = it->second;
      break;
    }
  }
  ast = ast->child_;
  vector<TableInfo *> tables;
  vector<TableInfo *>::iterator iter;
  Currentp->catalog_mgr_->GetTables(tables);
  for (iter = tables.begin(); iter != tables.end(); iter++) {
    vector<IndexInfo *> indexes;
    vector<IndexInfo *>::iterator i;
    Currentp->catalog_mgr_->GetTableIndexes((*iter)->GetTableName(), indexes);
    for (i = indexes.begin(); i != indexes.end(); i++) {
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
  std::unordered_map<std::string, DBStorageEngine *>::iterator it;
  for (it = dbs_.begin(); it != dbs_.end(); it++) {
    if (it->first == current_db_)
    {
      Currentp = it->second;
      break;
    }
  }
  Currentp->catalog_mgr_->GetTable(ast->val_, currenttable);
  Schema *columnToSelect = currenttable->GetSchema();
  /* 把要找的column放到columns里 */
  if (tmp->type_ == kNodeAllColumns) {
    columns = columnToSelect->GetColumns();
  } 
  else if (tmp->type_ == kNodeColumnList) {
    tmp = tmp->child_;
    while (tmp) {
      uint32_t columnindex = 0;
      Column nowColumn = columnToSelect->GetColumn(columnToSelect->GetColumnIndex(tmp->val_, columnindex));
      columns.push_back(&nowColumn);
      tmp = tmp->next_;
    }
  }
  ast = ast->next_;
  // 此处开始判断条件
  if (ast->type_ == kNodeConditions) {
    pSyntaxNode root= ast->child_;
    if (root->type_ == kNodeConnector) {
      
    }
    
  }
  return DB_FAILED;
}

dberr_t ExecuteEngine::ExecuteInsert(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteInsert" << std::endl;
#endif
  return DB_FAILED;
}

dberr_t ExecuteEngine::ExecuteDelete(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteDelete" << std::endl;
#endif
  return DB_FAILED;
}

dberr_t ExecuteEngine::ExecuteUpdate(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteUpdate" << std::endl;
#endif
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
