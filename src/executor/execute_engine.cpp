#include "executor/execute_engine.h"
#include <math.h>
#include <stdio.h>
#include <algorithm>
#include <fstream>
#include <stack>
#include <unordered_map>
#include "glog/logging.h"
#include "index/index_iterator.h"
#include "storage/table_iterator.h"
using namespace std;
extern "C" {
int yyparse(void);
extern FILE *yyin;
#include "parser/minisql_lex.h"
#include "parser/parser.h"
}
ExecuteEngine::ExecuteEngine() { isRecons = false; }

dberr_t ExecuteEngine::Execute(pSyntaxNode ast, ExecuteContext *context) {
  // 先从文件中把database重构
  if (isRecons == false) {
    ifstream in("databasefile.txt");
    string databasename;
    if (in.is_open()) {
      while (!in.eof()) {
        in >> databasename;
        DBStorageEngine *db = new DBStorageEngine(databasename, false);
        dbs_.insert(make_pair(databasename, db));
      }
      in.close();
    }
    isRecons = true;
  }
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
  // 先找以前有没有创建
  ifstream in("databasefile.txt");
  if (in.is_open()) {
    string tmpdatabasename;
    while (!in.eof()) {
      in >> tmpdatabasename;
      if (tmpdatabasename == ast->val_) {
        cout << "This database already exists." << endl;
        return DB_FAILED;
      }
    }
    in.close();
  }
  ofstream out("databasefile.txt", ios::app);
  if (out.is_open()) {
    DBStorageEngine *NewDBptr = new DBStorageEngine(ast->val_);
    dbs_.insert(make_pair(ast->val_, NewDBptr));

    out << ast->val_ << " ";
    out.close();
    return DB_SUCCESS;
  }
  return DB_FAILED;
}

dberr_t ExecuteEngine::ExecuteDropDatabase(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteDropDatabase" << std::endl;
#endif
  ast = ast->child_;
  // 先找到要被drop的database
  for (auto it = dbs_.begin(); it != dbs_.end(); it++) {
    if (it->first == ast->val_)  // 找到
    {
      DBStorageEngine *DBToDrop = it->second;

      ifstream in("databasefile.txt");
      ofstream outtmp("databasefiletmp.txt");
      
      if (in.is_open() && outtmp.is_open()) {
        while (!in.eof()) {
          string tmp;
          in >> tmp;
          if (tmp != ast->val_) {
            outtmp << tmp << " ";
          }
        }
        in.close();
        outtmp.close();
        
        remove("databasefile.txt");
        ofstream out("databasefile.txt");
        ifstream intmp("databasefiletmp.txt");
        if (out.is_open() && intmp.is_open()) {
          while (!intmp.eof()) {
            string tmp11;
            intmp >> tmp11;
            out << tmp11 << " ";
          }
          intmp.close();
          out.close();
          delete DBToDrop;      // 删除这个database
          it = dbs_.erase(it);  // 从unorderedmap中移除该dbs
          remove("databasefiletmp.txt");
          return DB_SUCCESS;
        }
      }
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
  if (dbs_.empty())  // 此时没有数据库
  {
    cout << "No database." << endl;
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
  // 找到database并把它作为current database
  for (auto it = dbs_.begin(); it != dbs_.end(); it++) {
    if (it->first == ast->val_)  // 找到
    {
      current_db_ = ast->val_;  // 作为current database
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
    if (Currentp->catalog_mgr_->GetTables(CurrentTable) == DB_FAILED) return DB_FAILED;
    // 遍历vector，输出每个表的名字
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
  /* 找到现在的DB */
  DBStorageEngine *Currentp;
  for (auto it = dbs_.begin(); it != dbs_.end(); it++) {
    if (it->first == current_db_)  // 找到
    {
      Currentp = it->second;
      break;
    }
  }
  vector<Column> primarykey;
  ast = ast->next_;  // ast type kNodeColumnDefinitionList
  if (ast->type_ == kNodeColumnDefinitionList) {
    ast = ast->child_;  // 遍历生成column的pSyntaxNode
    while (ast != NULL) {
      uniqueable = false;
      pSyntaxNode tmp = ast;
      if (ast->type_ == kNodeColumnDefinition) {
        tmp = tmp->child_;
        if (ast->val_ != NULL && strcmp(ast->val_, "unique") == 0) uniqueable = true;
        if (tmp->type_ == kNodeIdentifier)  // column name
        {
          if (strcmp(tmp->next_->val_, "int") == 0) {
            newtype = kTypeInt;
            // Column newcol(tmp->val_, newtype, indexnum, nullable, uniqueable);
            Column *newcolptr =
                ALLOC_P(Currentp->catalog_mgr_->GetHeap(), Column)(tmp->val_, newtype, indexnum, nullable, uniqueable);
            NewColumns.push_back(newcolptr);
          } else if (strcmp(tmp->next_->val_, "float") == 0) {
            newtype = kTypeFloat;
            // Column newcol(tmp->val_, newtype, indexnum, nullable, uniqueable);
            Column *newcolptr =
                ALLOC_P(Currentp->catalog_mgr_->GetHeap(), Column)(tmp->val_, newtype, indexnum, nullable, uniqueable);
            NewColumns.push_back(newcolptr);
          } else if (strcmp(tmp->next_->val_, "char") == 0) {
            newtype = kTypeChar;
            float l = atof(tmp->next_->child_->val_);
            // 此处应该增加约束条件
            uint32_t length = 0;
            if (ceil(l) != floor(l) || l < 0) {
              cout << "字符长度不是整数" << endl;
              return DB_FAILED;
            }
            if (l <= 0) {
              cout << "字符串长度<=0" << endl;
              return DB_FAILED;
            }
            length = ceil(l);
            // Column newcol(tmp->val_, newtype, length, indexnum, nullable, uniqueable);
            Column *newcolptr =
                ALLOC_P(Currentp->catalog_mgr_->GetHeap(), Column)(tmp->val_, newtype, length, indexnum, nullable, uniqueable);
            NewColumns.push_back(newcolptr);
          }
          indexnum++;
        }
      } else if (ast->type_ == kNodeColumnList) {
        if (strcmp(ast->val_, "primary keys") == 0) {
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
    IndexInfo *index_info = NULL;
    IndexInfo *index_info_pk = NULL;
    // TableSchema NewSchema(NewColumns);
    // TableSchema *p = new
    if (primarykey.size() == 1) {
      primarykey[0].SetUnique();
    }
    TableSchema *NewSchema = ALLOC_P(Currentp->catalog_mgr_->GetHeap(), TableSchema)(NewColumns);
    if (Currentp->catalog_mgr_->CreateTable(NewTableName, NewSchema, primarykey, txn, Newtable_info) == DB_SUCCESS) {
      // MemHeap *heap{}
      // 给unique的列都建索引
      TableInfo *currenttable{};
      Currentp->catalog_mgr_->GetTable(NewTableName, currenttable);
      vector<Column *> currentColumns;
      currentColumns =currenttable->GetSchema()->GetColumns();

      for (auto columnsiter = currentColumns.begin(); columnsiter != currentColumns.end(); columnsiter++) {
        if ((*columnsiter)->IsUnique()) {
          vector<string> indexkeys;
          indexkeys.push_back((*columnsiter)->GetName());
          Currentp->catalog_mgr_->CreateIndex(NewTableName, (*columnsiter)->GetName(), indexkeys, txn, index_info);
        }
        // unique即建索引
      }
      // pk建索引
      vector<Column> pkColumns = currenttable->GetPrimaryKey();
      vector<string> pkString;
      for (auto pkiter = pkColumns.begin(); pkiter != pkColumns.end(); pkiter++) {
        pkString.push_back((*pkiter).GetName());
      }
      Currentp->catalog_mgr_->CreateIndex(NewTableName, "primarykey", pkString, txn, index_info_pk);
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
    if (it->first == current_db_)  // 找到
    {
      Currentp = it->second;
      break;
    }
  }
  if (it != dbs_.end()) {
    return (Currentp->catalog_mgr_->DropTable(ast->val_));
  }
  cout << "Fail to drop the table" << endl;
  return DB_FAILED;
}

dberr_t ExecuteEngine::ExecuteShowIndexes(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteShowIndexes" << std::endl;
#endif
  DBStorageEngine *Currentp;
  vector<TableInfo *> tables;
  for (auto it = dbs_.begin(); it != dbs_.end(); it++) {
    if (it->first == current_db_) {
      Currentp = it->second;
      break;
    }
  }
  Currentp->catalog_mgr_->GetTables(tables);  // 把该db中的table放入tables
  if (ast->type_ == kNodeShowIndexes) {
    for (auto iter = tables.begin(); iter != tables.end(); iter++) {
      string tablename = (*iter)->GetTableName();
      vector<IndexInfo *> indexes_;
      Currentp->catalog_mgr_->GetTableIndexes(tablename, indexes_);
      // show indexes
      /*cout << "Table"
           << " "
           << "Non_unique"
           << " "
           << "Key_name"
           << " "
           << "Column name"
           << " "
           << "Index_type" << endl;*/
      for (auto it = indexes_.begin(); it != indexes_.end(); it++) {
        cout << tablename << " ";
        IndexSchema *indexc = (*it)->GetIndexKeySchema();
        vector<Column *> indexcolumns = indexc->GetColumns();
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
    if (it->first == current_db_) {
      Currentp = it->second;
      break;
    }
  }
  ast = ast->child_;
  indexname = ast->val_;
  ast = ast->next_;
  tablename = ast->val_;
  ast = ast->next_;
  TableInfo *currenttable;
  if (Currentp->catalog_mgr_->GetTable(tablename, currenttable) == DB_TABLE_NOT_EXIST) return DB_TABLE_NOT_EXIST;
  vector<Column*> columns = currenttable->GetSchema()->GetColumns();
  if (ast->type_ == kNodeColumnList) {
    pSyntaxNode tmp = ast->child_;
    while (tmp != NULL) {
      for (auto iter = columns.begin(); iter != columns.end(); iter++) {
        if (!(*iter)->IsUnique() && (*iter)->GetName() == ast->val_) {
          cout << "只能在唯一键上建立索引" << endl;
          return DB_FAILED;
        }
      }
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
    if (it->first == current_db_) {
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

dberr_t ExecuteEngine::NewTravel(DBStorageEngine *Currentp, TableInfo *currenttable, pSyntaxNode root,
                                 vector<RowId> *result) {
  Transaction *txn = NULL;
  if (root->type_ == kNodeConnector) {
    vector<RowId> left, right;
    vector<RowId>::iterator iterright;
    NewTravel(Currentp, currenttable, root->child_, &left);
    NewTravel(Currentp, currenttable, root->child_->next_, &right);
    if (strcmp(root->val_, "and") == 0) {
      for (auto iterleft = left.begin(); iterleft != left.end(); iterleft++) {
        if (find(right.begin(), right.end(), *iterleft) != right.end()) {
          (*result).push_back(*iterleft);
        }
      }
    } else if (strcmp(root->val_, "or") == 0) {
      (*result).insert((*result).end(), left.begin(), left.end());
      (*result).insert((*result).end(), right.begin(), right.end());

      for (auto resultit = (*result).begin(); resultit != (*result).end(); resultit++) {
        for (auto i2 = resultit + 1; i2 != (*result).end(); i2++) {
          if ((*i2) == (*resultit)) {
            (*result).erase(i2);
            break;
          }
        }
      }
    }
    if (!result->empty()) return DB_SUCCESS;
    return DB_FAILED;
  } else if (root->type_ == kNodeCompareOperator) {
    char *cmpoperator = root->val_;
    char *op1 = root->child_->val_;
    char *op2 = root->child_->next_->val_;
    // if key 上有index
    IndexInfo *nowindex = nullptr;
    vector<IndexInfo *> nowindexes;
    string indexname;
    Currentp->catalog_mgr_->GetTableIndexes(currenttable->GetTableName(), nowindexes);
    for (auto m = nowindexes.begin(); m != nowindexes.end(); m++) {
      if ((*m)->GetIndexKeySchema()->GetColumns().size() == 1) {
        if ((*m)->GetIndexKeySchema()->GetColumn(0)->GetName() == op1) {
          indexname = (*m)->GetIndexName();
        }
      }
    }
    // 存在该列的索引
    if (Currentp->catalog_mgr_->GetIndex(currenttable->GetTableName(), indexname, nowindex) != DB_INDEX_NOT_FOUND) {
      // vector<RowId> result;
      if (root->child_->next_->type_ == kNodeNumber || root->child_->next_->type_ == kNodeString) {
        uint32_t op1index;
        currenttable->GetSchema()->GetColumnIndex(op1, op1index);
        TypeId typeop1 = currenttable->GetSchema()->GetColumn(op1index)->GetType();
        vector<Field> keyrowfield;
        if (typeop1 == kTypeInt) {
          keyrowfield.push_back(Field(typeop1, atoi(op2)));
        } else if (typeop1 == kTypeFloat) {
          keyrowfield.push_back(Field(typeop1, (float)atof(op2)));
        } else if (typeop1 == kTypeChar) {
          keyrowfield.push_back(Field(typeop1, op2, strlen(op2), true));
        }
        Row keyrow(keyrowfield);
        vector<RowId> scanresult;
        int position{};
        page_id_t leaf_page_id{};
        nowindex->GetIndex()->ScanKey(keyrow, scanresult, position, leaf_page_id, txn);
        IndexIterator<GenericKey<32>, RowId, GenericComparator<32>> indexiter(leaf_page_id, position, Currentp->bpm_);
        auto indexptr =
            reinterpret_cast<BPlusTreeIndex<GenericKey<32>, RowId, GenericComparator<32>> *>((*nowindex).GetIndex());
        if (scanresult.size() == 0) return DB_FAILED;
        if (strcmp(cmpoperator, "=") == 0) {
          (*result).push_back(scanresult[0]);
          if (!(*result).empty()) return DB_SUCCESS;
          return DB_FAILED;
        } else if (strcmp(cmpoperator, ">=") == 0) {
          for (; indexiter != indexptr->GetEndIterator(); indexiter.operator++()) {
            (*result).push_back((*indexiter).second);
          }
          if (!(*result).empty()) return DB_SUCCESS;
          return DB_FAILED;
        } else if (strcmp(cmpoperator, ">") == 0) {
          indexiter.operator++();
          for (; indexiter != indexptr->GetEndIterator(); indexiter.operator++()) {
            (*result).push_back((*indexiter).second);
          }
          if (!(*result).empty()) return DB_SUCCESS;
          return DB_FAILED;
        } else if (strcmp(cmpoperator, "<=") == 0) {
          // IndexIterator<GenericKey<32>, RowId, GenericComparator<32>> indexiter2 = indexiter;
          IndexIterator<GenericKey<32>, RowId, GenericComparator<32>> indexiter2 = indexptr->GetBeginIterator();
          indexiter.operator++();
          for (; indexiter2 != indexiter; indexiter2.operator++()) {
            (*result).push_back((*indexiter2).second);
          }
          if (!(*result).empty()) return DB_SUCCESS;
          return DB_FAILED;
        } else if (strcmp(cmpoperator, "<") == 0) {
          // IndexIterator<GenericKey<32>, RowId, GenericComparator<32>> indexiter2 = indexiter;
          IndexIterator<GenericKey<32>, RowId, GenericComparator<32>> indexiter2 = indexptr->GetBeginIterator();

          for (; indexiter2 != indexiter; indexiter2.operator++()) {
            (*result).push_back((*indexiter2).second);
          }
          if (!(*result).empty()) return DB_SUCCESS;
          return DB_FAILED;
        } else if (strcmp(cmpoperator, "!=") == 0) {
          // IndexIterator<GenericKey<32>, RowId, GenericComparator<32>> indexiter2 = indexiter;
          IndexIterator<GenericKey<32>, RowId, GenericComparator<32>> indexiter2 = indexptr->GetBeginIterator();

          for (; indexiter2 != indexiter; indexiter2.operator++()) {
            (*result).push_back((*indexiter2).second);
          }
          indexiter.operator++();
          for (; indexiter != indexptr->GetEndIterator(); indexiter.operator++()) {
            (*result).push_back((*indexiter).second);
          }
          if (!(*result).empty()) return DB_SUCCESS;
          return DB_FAILED;
        }
      } else if (root->child_->next_->type_ == kNodeNull) {
        TableIterator tableit(currenttable->GetTableHeap()->Begin(txn));
        for (tableit == currenttable->GetTableHeap()->Begin(txn); tableit != currenttable->GetTableHeap()->End();
             tableit++) {
          if (TravelWithoutIndex(currenttable, tableit, root) == kTrue) {
            (*result).push_back((*tableit).GetRowId());
          }
        }
        if (!(*result).empty()) return DB_SUCCESS;
        return DB_FAILED;
      }
    }
    // 不存在该列的索引
    else {
      TableIterator tableit(currenttable->GetTableHeap()->Begin(txn));
      for (tableit == currenttable->GetTableHeap()->Begin(txn); tableit != currenttable->GetTableHeap()->End();
           tableit++) {
        if (TravelWithoutIndex(currenttable, tableit, root) == kTrue) {
          (*result).push_back((*tableit).GetRowId());
        }
      }
      if (!(*result).empty()) return DB_SUCCESS;
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
    if (currenttable->GetSchema()->GetColumnIndex(op1, op1index) == DB_SUCCESS) {
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
      } else if (strcmp(cmpoperator, ">") == 0) {
        CmpBool returnvalue = now->CompareGreaterThan(*pto);
        delete pto;
        return returnvalue;
      } else if (strcmp(cmpoperator, "<") == 0) {
        CmpBool returnvalue = now->CompareLessThan(*pto);
        delete pto;
        return returnvalue;
      } else if (strcmp(cmpoperator, "!=") == 0) {
        CmpBool returnvalue = now->CompareNotEquals(*pto);
        delete pto;
        return returnvalue;
      } else if (strcmp(cmpoperator, "<=") == 0) {
        CmpBool returnvalue = now->CompareLessThanEquals(*pto);
        delete pto;
        return returnvalue;
      } else if (strcmp(cmpoperator, ">=") == 0) {
        CmpBool returnvalue = now->CompareGreaterThanEquals(*pto);
        delete pto;
        return returnvalue;
      }
    }
    cout << "Wrong column name!" << endl;
    return kFalse;
  } else if (root->child_->next_->type_ == kNodeNull) {
    if (strcmp(cmpoperator, "is") == 0) {
      // is null
      Field *now{};
      uint32_t op1index{};
      if (currenttable->GetSchema()->GetColumnIndex(op1, op1index) == DB_SUCCESS) {
        now = (*tableit).GetField(op1index);
        return GetCmpBool(now->IsNull());
      }
      cout << "Wrong column name!" << endl;
      return kFalse;
    } else if (strcmp(cmpoperator, "not") == 0) {
      // not null
      Field *now{};
      uint32_t op1index{};
      if (currenttable->GetSchema()->GetColumnIndex(op1, op1index) == DB_SUCCESS) {
        now = (*tableit).GetField(op1index);
        return GetCmpBool(!(now->IsNull()));
      }
      cout << "Wrong column name!" << endl;
      return kFalse;
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
    } else if (strcmp(root->val_, "or") == 0) {
      if (Travel(currenttable, tableit, root->child_) == kTrue ||
          Travel(currenttable, tableit, root->child_->next_) == kTrue)
        return kTrue;
      return kFalse;
    }
    return kFalse;  // 可能还有别的Connector吧不管了现在这边返回一下
  } else if (root->type_ == kNodeCompareOperator) {
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
        } else if (typeop1 == kTypeFloat) {
          Field o(typeop1, (float)atof(op2));
          pto = &o;
        } else if (typeop1 == kTypeChar) {
          Field o(typeop1, op2, strlen(op2), true);
          pto = &o;
        }

        if (strcmp(cmpoperator, "=") == 0) {
          return now->CompareEquals(*pto);
        } else if (strcmp(cmpoperator, ">") == 0) {
          return now->CompareGreaterThan(*pto);
        } else if (strcmp(cmpoperator, "<") == 0) {
          return now->CompareLessThan(*pto);
        } else if (strcmp(cmpoperator, "!=") == 0) {
          return now->CompareNotEquals(*pto);
        } else if (strcmp(cmpoperator, "<=") == 0) {
          return now->CompareLessThanEquals(*pto);
        } else if (strcmp(cmpoperator, ">=") == 0) {
          return now->CompareGreaterThanEquals(*pto);
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
    if (it->first == current_db_) {
      Currentp = it->second;
      break;
    }
  }
  if (Currentp->catalog_mgr_->GetTable(ast->val_, currenttable) == DB_TABLE_NOT_EXIST) return DB_TABLE_NOT_EXIST;
  Schema *columnToSelect = currenttable->GetSchema();
  /* 把要找的column放到columns里 */
  if (tmp->type_ == kNodeAllColumns) {
    columns = columnToSelect->GetColumns();
  } else if (tmp->type_ == kNodeColumnList) {
    tmp = tmp->child_;
    while (tmp) {
      uint32_t columnindex = 0;
      if (columnToSelect->GetColumnIndex(tmp->val_, columnindex) == DB_COLUMN_NAME_NOT_EXIST)
        return DB_COLUMN_NAME_NOT_EXIST;
      columns.push_back(columnToSelect->GetColumn(columnindex));
      tmp = tmp->next_;
    }
  }
  ast = ast->next_;
  // 此处开始判断条件
  if (ast != NULL && ast->type_ == kNodeConditions) {
    pSyntaxNode root = ast->child_;
    vector<RowId> result;

    if (NewTravel(Currentp, currenttable, root, &result) == DB_SUCCESS) {
      for (auto i = result.begin(); i != result.end(); i++) {
        Row nowrow(*i);
        currenttable->GetTableHeap()->GetTuple(&nowrow, txn);
        for (auto fielditer = columns.begin(); fielditer != columns.end(); fielditer++) {
          uint32_t fieldid;
          currenttable->GetSchema()->GetColumnIndex((*fielditer)->GetName(), fieldid);

          std::cout << nowrow.GetField(fieldid)->GetData();
        }
        std::cout << std::endl;
      }
    }
    return DB_SUCCESS;
  } else if (ast == NULL) {  // 没有条件
    TableIterator tableit(currenttable->GetTableHeap()->Begin(txn));
    for (/*tableit == currenttable->GetTableHeap()->Begin(txn)*/; tableit != currenttable->GetTableHeap()->End();
         tableit++) {
      // 打印
      Row row((*tableit).GetRowId());
      currenttable->GetTableHeap()->GetTuple(&row, txn);

      for (auto i = columns.begin(); i != columns.end(); i++) {
        uint32_t tmpcolumnindex{};
        currenttable->GetSchema()->GetColumnIndex((*i)->GetName(), tmpcolumnindex);
        Field *tmpField = row.GetField(tmpcolumnindex);
        if (tmpField->IsNull())
          cout << "null"
               << " ";
        else
          cout << tmpField->GetData() << " ";
      }
      // row.GetField();
      cout << endl;
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
  /* 找到当前db */
  for (auto it = dbs_.begin(); it != dbs_.end(); it++) {
    if (it->first == current_db_)  // 找到
    {
      Currentp = it->second;
      break;
    }
  }
  ast = ast->child_;
  /* 找到要被插入的表 */
  if (ast->type_ == kNodeIdentifier) {
    if (Currentp->catalog_mgr_->GetTable(ast->val_, currenttable) == DB_TABLE_NOT_EXIST) return DB_TABLE_NOT_EXIST;
  }
  ast = ast->next_;  // kColumnValues
  ast = ast->child_;
  vector<Field> newfield;
  vector<Column *> columns = currenttable->GetSchema()->GetColumns();
  while (ast != NULL) {
    // Field tmpField()
    if (ast->type_ == kNodeNumber) {
      if (strchr(ast->val_, '.') != NULL)  // float
      {
        Field tmpField(kTypeFloat, (float)atof(ast->val_));
        if (columns.size() > newfield.size() && columns[newfield.size()]->GetType() != kTypeFloat) {
          cout << "Wrong Type!" << endl;
          return DB_FAILED;
        }
        newfield.push_back(tmpField);
      } else {  // integer or float

        if (columns.size() > newfield.size() && columns[newfield.size()]->GetType() == kTypeInt) {
          Field tmpField(kTypeInt, atoi(ast->val_));
          newfield.push_back(tmpField);
        } else if (columns.size() > newfield.size() && columns[newfield.size()]->GetType() == kTypeFloat) {
          Field tmpField(kTypeFloat, (float)atoi(ast->val_));
          newfield.push_back(tmpField);
        } else {
          cout << "Wrong Type!" << endl;
          return DB_FAILED;
        }
      }
    } else if (ast->type_ == kNodeString) {
      Field tmpField(kTypeChar, ast->val_, strlen(ast->val_), true);
      if (columns.size() > newfield.size() && columns[newfield.size()]->GetType() != kTypeChar) {
        cout << "Wrong Type!" << endl;
        return DB_FAILED;
      }
      newfield.push_back(tmpField);
    } else if (ast->type_ == kNodeNull) {
      if (ast->id_ > (int)columns.size()) {
        cout << "Wrong Columns num!" << endl;
        return DB_FAILED;
      }
      TypeId tmptype = currenttable->GetSchema()->GetColumn(ast->id_ - 1)->GetType();
      if (currenttable->GetSchema()->GetColumn(ast->id_ - 1)->IsNullable() == false) {
        cout << "不可以为空" << endl;
        return DB_FAILED;
      }
      Field tmpField(tmptype);
      newfield.push_back(tmpField);
    }
    ast = ast->next_;
  }
  Row row(newfield);

  Currentp->catalog_mgr_->GetTableIndexes(currenttable->GetTableName(), indexes);
  // 检查newfield是否符合插入条件
  if (row.GetFieldCount() != columns.size()) {
    cout << "Wrong Input!" << endl;
    return DB_FAILED;
  }
  // 检查unique
  bool check = false;
  vector<Column *> uniqueColumns;
  for (auto columnsiter = columns.begin(); columnsiter != columns.end(); columnsiter++) {
    check = false;
    if ((*columnsiter)->IsUnique()) {
      // 如果该列上有index
      for (auto iterindexes = indexes.begin(); iterindexes != indexes.end(); iterindexes++) {
        if ((*iterindexes)->GetIndexKeySchema()->GetColumn(0)->GetName() == (*columnsiter)->GetName()) {
          // 通过index找有无重复
          vector<RowId> result;
          int position;
          page_id_t leaf_page_id;

          uint32_t keyindex;
          currenttable->GetSchema()->GetColumnIndex((*columnsiter)->GetName(), keyindex);
          vector<Field> rowkeyfield;
          rowkeyfield.push_back(*row.GetField(keyindex));
          Row rowkey(rowkeyfield);
          if ((*iterindexes)->GetIndex()->ScanKey(rowkey, result, position, leaf_page_id, txn) == DB_SUCCESS) {
            cout << "对于Unique列，不应该插入重复的元组" << endl;
            return DB_FAILED;
          }
          check = true;
          break;
        }
      }
      // 如果该列上没有index，用tableiterator来检查有无重复tuple
      if (check == false) {
        TableIterator tableit(currenttable->GetTableHeap()->Begin(txn));
        for (tableit == currenttable->GetTableHeap()->Begin(txn); tableit != currenttable->GetTableHeap()->End();
             tableit++) {
          uint32_t indexop1{};
          currenttable->GetSchema()->GetColumnIndex((*columnsiter)->GetName(), indexop1);
          Field *currentfield = (*tableit).GetField(indexop1);
          if (currentfield->CompareEquals(newfield[indexop1]) == kTrue) {
            cout << "对于Unique列，不应该插入重复的元组" << endl;
            return DB_FAILED;
          }
        }
      }
    }
  }
  vector<Column> primarykey = currenttable->GetPrimaryKey();
  // 如果是主键 则检查有无重复元组
  if (primarykey.size() > 1) {
    vector<uint32_t> columnindexes;
    vector<uint32_t>::iterator iter;
    for (auto piter = primarykey.begin(); piter != primarykey.end(); piter++) {
      uint32_t tmpindex;
      currenttable->GetSchema()->GetColumnIndex((*piter).GetName(), tmpindex);
      columnindexes.push_back(tmpindex);
    }
    // 此时说明是联合主键
    //TableIterator tableit(currenttable->GetTableHeap()->Begin(txn));
    //for (tableit == currenttable->GetTableHeap()->Begin(txn); tableit != currenttable->GetTableHeap()->End();
    //     tableit++) {
    //  for (iter = columnindexes.begin(); iter != columnindexes.end(); iter++) {
    //    if (row.GetField(*iter)->CompareEquals(*((*tableit).GetField(*iter))) != kTrue) {
    //      break;
    //    }
    //  }
    //  if (iter == columnindexes.end())  // 所有的field都一样
    //  {
    //    cout << "conflict with primary key!" << endl;
    //    return DB_FAILED;
    //  }
    //}

    /*find the index for pk*/
    for (auto iterindexes = indexes.begin(); iterindexes != indexes.end(); iterindexes++) {
      if ((*iterindexes)->GetIndexName() == "primarykey") {
        vector<RowId> result;
        int position;
        page_id_t leaf_page_id;

        vector<Field> rowkeyfield;
        /*get the key value of pk*/
        for (auto it = columnindexes.begin(); it != columnindexes.end(); it++) {
          rowkeyfield.push_back(*row.GetField(*it));
        }
        Row rowkey(rowkeyfield);
        if ((*iterindexes)->GetIndex()->ScanKey(rowkey, result, position, leaf_page_id, txn) == DB_SUCCESS) {
          cout << "对于primary key列，不应该插入重复的元组" << endl;
          return DB_FAILED;
        }
        break;
      }
    }
  }

  if (currenttable->GetTableHeap()->InsertTuple(row, txn)) {
    // 更新tablemeta root_pageid
    currenttable->SetRootPageId();
    // 检查indexex
    for (auto iterindexes = indexes.begin(); iterindexes != indexes.end(); iterindexes++) {
      uint32_t keyindex;
      uint32_t i = 0;
      vector<Field> rowkeyfield;
      for (i = 0; i < (*iterindexes)->GetIndexKeySchema()->GetColumnCount(); i++) {
        currenttable->GetSchema()->GetColumnIndex((*iterindexes)->GetIndexKeySchema()->GetColumn(i)->GetName(),
                                                  keyindex);
        rowkeyfield.push_back(*row.GetField(keyindex));
      }
      Row rowkey(rowkeyfield);
      if ((*iterindexes)->GetIndex()->InsertEntry(rowkey, row.GetRowId(), txn) == DB_FAILED) return DB_FAILED;
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
  /* 找到当前db */
  for (auto it = dbs_.begin(); it != dbs_.end(); it++) {
    if (it->first == current_db_)  // 找到
    {
      Currentp = it->second;
      break;
    }
  }
  ast = ast->child_;
  /* 找到要被删除的表 */
  if (ast->type_ == kNodeIdentifier) {
    if (Currentp->catalog_mgr_->GetTable(ast->val_, currenttable) == DB_TABLE_NOT_EXIST) return DB_TABLE_NOT_EXIST;
  }
  // 全表删除
  if (ast->next_ == NULL) {
    TableIterator tableit(currenttable->GetTableHeap()->Begin(txn));
    for (tableit == currenttable->GetTableHeap()->Begin(txn); tableit != currenttable->GetTableHeap()->End();
         tableit++) {
      if (currenttable->GetTableHeap()->MarkDelete((*tableit).GetRowId(), txn) == false) return DB_FAILED;
      currenttable->GetTableHeap()->ApplyDelete((*tableit).GetRowId(), txn);
    }
    // 更新pagerootid
    currenttable->SetRootPageId();
    return DB_SUCCESS;
  }
  ast = ast->next_;  // kNodeConditions
  vector<IndexInfo *> indexes;
  Currentp->catalog_mgr_->GetTableIndexes(currenttable->GetTableName(), indexes);
  if (ast->type_ == kNodeConditions) {
    pSyntaxNode root = ast->child_;
    vector<RowId> result;
    if (NewTravel(Currentp, currenttable, root, &result) == DB_SUCCESS) {
      for (auto iterresult = result.begin(); iterresult != result.end(); iterresult++) {
        Row nowrow(*iterresult);
        currenttable->GetTableHeap()->GetTuple(&nowrow, txn);
        currenttable->GetTableHeap()->MarkDelete((*iterresult), txn);
        currenttable->GetTableHeap()->ApplyDelete((*iterresult), txn);
        // 更新pagerootid
        currenttable->SetRootPageId();
        for (auto iterindexes = indexes.begin(); iterindexes != indexes.end(); iterindexes++) {
          uint32_t keyindex;
          currenttable->GetSchema()->GetColumnIndex((*iterindexes)->GetIndexKeySchema()->GetColumn(0)->GetName(),
                                                    keyindex);
          vector<Field> rowkeyfield;
          rowkeyfield.push_back(*nowrow.GetField(keyindex));
          Row rowkey(rowkeyfield);
          if ((*iterindexes)->GetIndex()->RemoveEntry(rowkey, (*iterresult), txn) == DB_FAILED) return DB_FAILED;
        }
      }
      return DB_SUCCESS;
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
  /* 找到当前db */
  for (auto it = dbs_.begin(); it != dbs_.end(); it++) {
    if (it->first == current_db_)  // 找到
    {
      Currentp = it->second;
      break;
    }
  }
  ast = ast->child_;
  /* 找到要被更新的表 */
  if (ast->type_ == kNodeIdentifier) {
    if (Currentp->catalog_mgr_->GetTable(ast->val_, currenttable) == DB_TABLE_NOT_EXIST) return DB_TABLE_NOT_EXIST;
  }
  vector<Field> update;
  vector<uint32_t> FieldColumn;
  vector<Column *> columns = currenttable->GetSchema()->GetColumns();
  int updatecolumn = 0;
  ast = ast->next_;  // kNodeUpdateValues
  pSyntaxNode record = ast;
  vector<Column> primarykey = currenttable->GetPrimaryKey();
  if (ast->type_ == kNodeUpdateValues) {
    ast = ast->child_;
    while (ast != NULL) {
      char *op1 = ast->child_->val_;
      uint32_t op1index;
      if (currenttable->GetSchema()->GetColumnIndex(op1, op1index) == DB_COLUMN_NAME_NOT_EXIST)
        return DB_COLUMN_NAME_NOT_EXIST;
      FieldColumn.push_back(op1index);
      char *op2 = ast->child_->next_->val_;
      if (ast->child_->next_->type_ == kNodeNumber) {
        if (strchr(op2, '.') != NULL)  // float
        {
          Field tmpField(kTypeFloat, (float)atof(op2));
          if (columns.size() > update.size() && columns[op1index]->GetType() != kTypeFloat) {
            cout << "Wrong Type!" << endl;
            return DB_FAILED;
          }
          update.push_back(tmpField);
        } else {  // integer or float
          Field tmpField(kTypeInt, atoi(op2));
          if (columns.size() > update.size() && columns[op1index]->GetType() == kTypeInt) {
            Field tmpField(kTypeInt, atoi(op2));
            update.push_back(tmpField);

          } else if (columns.size() > update.size() && columns[op1index]->GetType() == kTypeFloat) {
            Field tmpField(kTypeFloat, (float)atof(op2));
            update.push_back(tmpField);
          } else {
            cout << "Wrong Type!" << endl;
            return DB_FAILED;
          }
        }
      } else if (ast->child_->next_->type_ == kNodeString) {
        Field tmpField(kTypeChar, op2, strlen(op2), true);
        if (columns.size() > update.size() && columns[op1index]->GetType() != kTypeChar) {
          cout << "Wrong Type!" << endl;
          return DB_FAILED;
        }
        update.push_back(tmpField);
      } else if (ast->child_->next_->type_ == kNodeNull) {
        if (ast->id_ > (int)columns.size()) {
          cout << "Wrong Columns num!" << endl;
          return DB_FAILED;
        }
        TypeId tmptype = currenttable->GetSchema()->GetColumn(ast->child_->next_->id_ - 1)->GetType();
        if (currenttable->GetSchema()->GetColumn(ast->child_->next_->id_ - 1)->IsNullable() == false) {
          cout << "不可以为空" << endl;
          return DB_FAILED;
        }
        Field tmpField(tmptype);
        update.push_back(tmpField);
      }
      ast = ast->next_;
    }
  }
  pSyntaxNode astCondition = record->next_;  // kNodeConditions
  vector<IndexInfo *> indexes;
  Currentp->catalog_mgr_->GetTableIndexes(currenttable->GetTableName(), indexes);
  if (astCondition != NULL && astCondition->type_ == kNodeConditions) {
    pSyntaxNode root = astCondition->child_;
    vector<RowId> result;
    if (NewTravel(Currentp, currenttable, root, &result) == DB_SUCCESS) {
      for (auto iterresult = result.begin(); iterresult != result.end(); iterresult++) {
        updatecolumn = 0;

        Row rowp(*iterresult);
        currenttable->GetTableHeap()->GetTuple(&rowp, txn);
        Row previous = rowp;
        vector<Field *> pre = previous.GetFields();

        for (auto updateiter = update.begin(); updateiter != update.end(); updateiter++) {
          Field tmp(*updateiter);
          Swap((*updateiter), *(pre[FieldColumn[updatecolumn]]));
          Swap((*updateiter), tmp);
          updatecolumn++;
        }
        Row nowrow(*iterresult);
        currenttable->GetTableHeap()->GetTuple(&nowrow, txn);
        // 检查unique约束
        bool check = false;
        vector<Column *> uniqueColumns;
        for (auto columnsiter = columns.begin(); columnsiter != columns.end(); columnsiter++) {
          check = false;
          if ((*columnsiter)->IsUnique()) {
            // 如果该列上有index
            for (auto iterindexes = indexes.begin(); iterindexes != indexes.end(); iterindexes++) {
              if ((*iterindexes)->GetIndexKeySchema()->GetColumn(0)->GetName() == (*columnsiter)->GetName()) {
                // 通过index找有无重复
                vector<RowId> Scanresult;

                int position;
                page_id_t leaf_page_id;
                uint32_t keyindex;
                currenttable->GetSchema()->GetColumnIndex((*columnsiter)->GetName(), keyindex);
                vector<Field> rowkeyfield;
                rowkeyfield.push_back(*previous.GetField(keyindex));
                Row rowkey(rowkeyfield);
                if ((*iterindexes)->GetIndex()->ScanKey(rowkey, Scanresult, position, leaf_page_id, txn) ==
                    DB_SUCCESS) {
                  // if (Scanresult[0])
                  if (*iterresult == Scanresult[0]) {
                    continue;
                  }
                  cout << "对于Unique列，不应该插入重复的元组" << endl;
                  return DB_FAILED;
                }
                check = true;
                break;
              }
            }
            // 如果该列上没有index，用tableiterator来检查有无重复tuple
            if (check == false) {
              TableIterator tableit(currenttable->GetTableHeap()->Begin(txn));
              for (tableit == currenttable->GetTableHeap()->Begin(txn); tableit != currenttable->GetTableHeap()->End();
                   tableit++) {
                if ((*tableit).GetRowId() == (*iterresult)) continue;
                uint32_t indexop1{};
                currenttable->GetSchema()->GetColumnIndex((*columnsiter)->GetName(), indexop1);
                Field *currentfield = (*tableit).GetField(indexop1);
                if (currentfield->CompareEquals(*pre[indexop1]) == kTrue) {
                  cout << "对于Unique列，不应该插入重复的元组" << endl;
                  return DB_FAILED;
                }
              }
            }
          }
        }
        // primary key约束
        if (primarykey.size() > 1) {
          vector<uint32_t> columnindexes;
          vector<uint32_t>::iterator iter;
          for (auto piter = primarykey.begin(); piter != primarykey.end(); piter++) {
            uint32_t tmpindex;
            currenttable->GetSchema()->GetColumnIndex((*piter).GetName(), tmpindex);
            columnindexes.push_back(tmpindex);
          }
          // 此时说明是联合主键
          currenttable->GetTableHeap()->GetTuple(&nowrow, txn);
          TableIterator tableit(currenttable->GetTableHeap()->Begin(txn));
          for (tableit == currenttable->GetTableHeap()->Begin(txn); tableit != currenttable->GetTableHeap()->End();
               tableit++) {
            if ((*tableit).GetRowId() == (*iterresult)) continue;
            for (iter = columnindexes.begin(); iter != columnindexes.end(); iter++) {
              if (previous.GetField(*iter)->CompareEquals(*((*tableit).GetField(*iter))) != kTrue) {
                break;
              }
            }
            if (iter == columnindexes.end())  // 所有的field都一样
            {
              cout << "conflict with primary key!" << endl;
              return DB_FAILED;
            }
          }
        }
        if (currenttable->GetTableHeap()->UpdateTuple(previous, (*iterresult), txn) == false) return DB_FAILED;
        // 更新rootpageid
        currenttable->SetRootPageId();
        // 修改index
        for (auto iterindexes = indexes.begin(); iterindexes != indexes.end(); iterindexes++) {
          uint32_t keyindex;
          currenttable->GetSchema()->GetColumnIndex((*iterindexes)->GetIndexKeySchema()->GetColumn(0)->GetName(),
                                                    keyindex);
          vector<Field> rowkeyfield;
          rowkeyfield.push_back(*previous.GetField(keyindex));
          Row rowkey(rowkeyfield);
          if ((*iterindexes)->GetIndex()->RemoveEntry(rowkey, previous.GetRowId(), txn) == DB_FAILED) return DB_FAILED;
          if ((*iterindexes)->GetIndex()->InsertEntry(rowkey, previous.GetRowId(), txn) == DB_FAILED) return DB_FAILED;
        }
      }
      return DB_SUCCESS;
    }
    cout << "No Such Rows!" << endl;
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
  auto file_name = ast->child_->val_;
  ifstream fin;
  fin.open(file_name, ios::in);
  if (!fin.is_open()) {
    cout << "fail to open the file!" << endl;
    return DB_FAILED;
  }
  string buff;
  while (getline(fin, buff)) {
    YY_BUFFER_STATE bp = yy_scan_string(buff.c_str());
    if (bp == nullptr) {
      LOG(ERROR) << "Failed to create yy buffer state." << std::endl;
      exit(1);
    }

    yy_switch_to_buffer(bp);

    // init parser module
    MinisqlParserInit();

    // parse
    yyparse();

    // parse result handle
    if (MinisqlParserGetError()) {
      // error
      printf("%s\n", MinisqlParserGetErrorMessage());
    } else {
#ifdef ENABLE_PARSER_DEBUG
      printf("[INFO] Sql syntax parse ok!\n");
      SyntaxTreePrinter printer(MinisqlGetParserRootNode());
      printer.PrintTree(syntax_tree_file_mgr[syntax_tree_id++]);
#endif
    }
    ExecuteContext context;
    LOG(INFO) << "execute";
    (*this).Execute(MinisqlGetParserRootNode(), &context);

    // sleep(1);

    // clean memory after parse
    MinisqlParserFinish();
    yy_delete_buffer(bp);
    yylex_destroy();

    // quit condition
    if (context.flag_quit_) {
      printf("bye!\n");
      break;
    }
  }
  fin.close();
  return DB_FAILED;

}

dberr_t ExecuteEngine::ExecuteQuit(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteQuit" << std::endl;
#endif
  ASSERT(ast->type_ == kNodeQuit, "Unexpected node type.");
  context->flag_quit_ = true;
  /*for (auto it = dbs_.begin(); it != dbs_.end(); it++) {
    delete (*it).second;
  }*/
  return DB_SUCCESS;
}
