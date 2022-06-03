#include "catalog/catalog.h"

void CatalogMeta::SerializeTo(char *buf) const {
  // ASSERT(false, "Not Implemented yet");
  char *begin = buf;
  MACH_WRITE_UINT32(buf, CATALOG_METADATA_MAGIC_NUM);
  buf += sizeof(uint32_t);
  uint32_t size = table_meta_pages_.size();
  MACH_WRITE_UINT32(buf, size);
  buf += sizeof(uint32_t);
  for (auto i = table_meta_pages_.begin(); i != table_meta_pages_.end(); i++) {
    MACH_WRITE_UINT32(buf, i->first);
    buf += sizeof(uint32_t);
    MACH_WRITE_UINT32(buf, i->second);
    buf += sizeof(uint32_t);
  }
  MACH_WRITE_UINT32(buf, table_meta_pages_.end()->first);
  buf += sizeof(uint32_t);
  MACH_WRITE_UINT32(buf, table_meta_pages_.end()->second);
  buf += sizeof(uint32_t);
  
  size = index_meta_pages_.size();
  MACH_WRITE_UINT32(buf, size);
  buf += sizeof(uint32_t);
  for (auto i = index_meta_pages_.begin(); i != index_meta_pages_.end(); i++) {
    MACH_WRITE_UINT32(buf, i->first);
    buf += sizeof(uint32_t);
    MACH_WRITE_UINT32(buf, i->second);
    buf += sizeof(uint32_t);
  }
  MACH_WRITE_UINT32(buf, index_meta_pages_.end()->first);
  buf += sizeof(uint32_t);
  MACH_WRITE_UINT32(buf, index_meta_pages_.end()->second);
  buf += sizeof(uint32_t);
  buf = begin;
}

CatalogMeta *CatalogMeta::DeserializeFrom(char *buf, MemHeap *heap) {
  char *begin = buf;
  uint32_t magic_num = MACH_READ_UINT32(buf);
  buf += sizeof(uint32_t);
  if (magic_num != CATALOG_METADATA_MAGIC_NUM) {
    LOG(WARNING) << "MAGIC_NUM wrong in catalog Deserialize" << std::endl;
    buf = begin;
    return 0;
  }
  //需要反序列化得到两个map
  CatalogMeta *catalog = NewInstance(heap);
  uint32_t size = MACH_READ_UINT32(buf);
  buf += sizeof(uint32_t);
  for (uint32_t i = 0; i < size; i++) {
    table_id_t k1=MACH_READ_UINT32(buf);
    buf += sizeof(uint32_t);
    page_id_t k2 = MACH_READ_UINT32(buf);
    buf += sizeof(uint32_t);
    catalog->table_meta_pages_.insert(pair<table_id_t, page_id_t>(k1, k2));
  }
  size = MACH_READ_UINT32(buf);
  buf += sizeof(uint32_t);
  for (uint32_t i = 0; i < size; i++) {
    table_id_t k1 = MACH_READ_UINT32(buf);
    buf += sizeof(uint32_t);
    page_id_t k2 = MACH_READ_UINT32(buf);
    buf += sizeof(uint32_t);
    catalog->index_meta_pages_.insert(pair<table_id_t, page_id_t>(k1, k2));
  }
  buf = begin;
  return catalog;
}

uint32_t CatalogMeta::GetSerializedSize() const {
  return sizeof(uint32_t) * (3+2*(index_meta_pages_.size()+table_meta_pages_.size()));
}

CatalogMeta::CatalogMeta() {}


CatalogManager::CatalogManager(BufferPoolManager *buffer_pool_manager, LockManager *lock_manager,
                               LogManager *log_manager, bool init)
        : buffer_pool_manager_(buffer_pool_manager), lock_manager_(lock_manager),
          log_manager_(log_manager), heap_(new SimpleMemHeap()) {
  /*if init is true, we will clear all the element in catalogManager*/
  if (init) {
    /*initialize the catalog_meta_*/
    catalog_meta_ = CatalogMeta::NewInstance(heap_);
    next_table_id_ = -1;
    next_index_id_ = -1;
    /*other members are default constructed*/

    //catalog_meta_ = NULL;
    //next_table_id_ = -1;
    //next_index_id_ = -1;
    //table_names_.clear();
    //tables_.clear();
    //index_names_.clear();
    //indexes_.clear();//将各个元素清空初始化
    //Page *p = buffer_pool_manager->FetchPage(CATALOG_META_PAGE_ID);//找到catalog序列化存入的逻辑页
    //catalog_meta_->SerializeTo(p->GetData());//将他序列化的结果存到逻辑页的数据里面
  } else {
    /*catalog_meta deserialization*/
    Page *catalog_meta_page = buffer_pool_manager_->FetchPage(CATALOG_META_PAGE_ID);
    catalog_meta_ = CatalogMeta::DeserializeFrom(catalog_meta_page->GetData(), heap_);
    buffer_pool_manager_->UnpinPage(CATALOG_META_PAGE_ID, true);

    /*deserialize all the tableinfo*/
    for (auto table_page = catalog_meta_->GetTableMetaPages()->begin();
         table_page != catalog_meta_->GetTableMetaPages()->end(); table_page++) {
      Page *table_meta_page = buffer_pool_manager_->FetchPage(table_page->second);
      /*metadata for tableinfo*/
      TableMetadata *table_meta = nullptr;
      TableMetadata::DeserializeFrom(table_meta_page->GetData(),table_meta, heap_);
      buffer_pool_manager_->UnpinPage(table_page->second, true);
      /*tableheap for tableinfo*/
      TableHeap *table_heap = TableHeap::Create(buffer_pool_manager_, table_meta->GetFirstPageId(),
                                                table_meta->GetSchema(), log_manager, lock_manager, heap_);

      TableInfo *table_info = TableInfo::Create(heap_);
      table_info->Init(table_meta,table_heap);

      /*update the map for tables*/
      table_names_.insert(make_pair(table_meta->GetTableName(), table_meta->GetTableId()));
      tables_.insert(make_pair(table_meta->GetTableId(), table_info));

    }
    /*deserialize all the indexinfo*/
    for (auto index_page = catalog_meta_->GetIndexMetaPages()->begin();
         index_page != catalog_meta_->GetIndexMetaPages()->end(); index_page++) {
      Page *index_meta_page = buffer_pool_manager_->FetchPage(index_page->second);
      /*metadata for indexinfo*/
      IndexMetadata *index_meta = nullptr;
      IndexMetadata::DeserializeFrom(index_meta_page->GetData(), index_meta, heap_);
      buffer_pool_manager_->UnpinPage(index_page->second, true);

      TableInfo *table_info = tables_.find(index_meta->GetTableId())->second;
      IndexInfo *index_info = IndexInfo::Create(heap_);
      index_info->Init(index_meta,)

    }
  } 


}

CatalogManager::~CatalogManager() {
  delete heap_;
}

dberr_t CatalogManager::CreateTable(const string &table_name, TableSchema *schema,
                                    Transaction *txn, TableInfo *&table_info) {

  /*first: check if there is table_name already*/
  auto itcheck = table_names_.find(table_name);
  if (itcheck!=table_names_.end()) {
    return DB_TABLE_ALREADY_EXIST;
  }
  next_table_id_ = catalog_meta_->GetNextTableId() + 1;//找到下一个的id
  //找到root_page_id
  page_id_t meta_page_id = INVALID_PAGE_ID;
  Page *meta_page = buffer_pool_manager_->NewPage(meta_page_id);//meta data for this table
  page_id_t root_page_id = INVALID_PAGE_ID;
  Page *root_page = buffer_pool_manager_->NewPage(root_page_id);  //content for this table

  /*create metadata for this table*/
  TableMetadata *table_meta = TableMetadata::Create(next_table_id_, table_name, root_page_id, schema, heap_);  
  table_meta->SerializeTo(meta_page->GetData());//序列化这个元信息

  /*insert table_id ,meta page id pair in catalog meta data*/
  LoadTable(next_table_id_, meta_page_id);

  FlushCatalogMetaPage();

  /*write the tableinfo*/
  table_info = TableInfo::Create(heap_);
  TableHeap *table_heap;
  table_heap =TableHeap::Create(buffer_pool_manager_, schema, txn, log_manager_, lock_manager_, heap_);
  table_info->Init(table_meta, table_heap);

  /*update the info in catalog manager*/
  table_names_.insert(pair<string, table_id_t>(table_name, next_table_id_));
  tables_.insert(pair<table_id_t, TableInfo *>(next_table_id_, table_info));
}

dberr_t CatalogManager::GetTable(const string &table_name, TableInfo *&table_info) {
  /*find pair<tablename, tableid>*/
  auto itpair = table_names_.find(table_name);
  if (itpair == table_names_.end()) {
    return DB_TABLE_NOT_EXIST;
  }
  auto table_id=itpair->second;
  table_info = tables_.find(table_id)->second;
  return DB_SUCCESS;
}

dberr_t CatalogManager::GetTables(vector<TableInfo *> &tables) const {
  
  /*get out all the tableinfo*/  
  
  for (auto it = tables_.begin(); it != tables_.end(); it++) {
    tables.push_back(it->second);
  }
  return DB_SUCCESS;
}

dberr_t CatalogManager::CreateIndex(const std::string &table_name, const string &index_name,
                                    const std::vector<std::string> &index_keys, Transaction *txn,
                                    IndexInfo *&index_info) {
  /*first check if there is the table*/
   auto check_table=table_names_.find(table_name);
  if (check_table == table_names_.end()) {
     return DB_TABLE_NOT_EXIST;
  }
  /*then check if the index on this table exist*/
  auto check_index = (index_names_.find(table_name)->second).find(index_name);
  if (check_index != (index_names_.find(table_name)->second).end()) {
    return DB_INDEX_ALREADY_EXIST;
  }
  
  table_id_t table_id = table_names_[table_name];
  index_id_t next_index_id = catalog_meta_->GetNextIndexId() + 1;

  /*the same as create table : new a page for metadata and a page for index content */

  page_id_t meta_page_id=INVALID_PAGE_ID;
  Page *meta_page = buffer_pool_manager_->NewPage(meta_page_id);
 
  /*create a key_map for index constructor*/
  TableInfo *table_info = tables_[table_id];
  vector<uint32_t> key_map;
  uint32_t column_index = -1;
  for (auto it = index_keys.begin(); it != index_keys.end(); it++) {
    if (table_info->GetSchema()->GetColumnIndex(*it, column_index) == DB_SUCCESS
        &&table_info->GetSchema()->GetColumn(column_index)->IsUnique()) {
      key_map.push_back(column_index);
    } else {
      /*this index_keys doesn't exist or is is not unique*/
      return DB_FAILED;
    }
  }
  /*create indexmeta data*/
  IndexMetadata *index_meta = IndexMetadata::Create(next_index_id, index_name, table_id, key_map, heap_);
  index_meta->SerializeTo(meta_page->GetData());

  LoadIndex(next_index_id_, meta_page_id);
  FlushCatalogMetaPage();

  /*update the indexinfo*/
  index_info = IndexInfo::Create(heap_);
  index_info->Init(index_meta, table_info, buffer_pool_manager_);

  auto it = index_names_.find(table_name);
  (it->second).insert(pair<string, index_id_t>(index_name, next_index_id_));
  indexes_.insert(pair<index_id_t, IndexInfo *>(next_index_id_, index_info));
}
dberr_t CatalogManager::GetIndex(const std::string &table_name, const std::string &index_name,
                                 IndexInfo *&index_info) const {
  auto table_indexmap = index_names_.find(table_name);  
  if (table_indexmap == index_names_.end()) {
    return DB_TABLE_NOT_EXIST;
  }
  auto indexname_id = (table_indexmap->second).find(index_name);
  if (indexname_id == (table_indexmap->second).end()) {
    return DB_INDEX_NOT_FOUND;
  }
  index_info = indexes_.find(indexname_id->second)->second;
  
  return DB_SUCCESS;
}

dberr_t CatalogManager::GetTableIndexes(const std::string &table_name, std::vector<IndexInfo *> &indexes) const {
  
  auto table_index = index_names_.find(table_name);
  if (table_index == index_names_.end()) {
    return DB_TABLE_NOT_EXIST;
  }
  /*for all pair in <string, index_id_t>*/
  for (auto itindex = (table_index->second).begin(); itindex != (table_index->second).end(); itindex++) {
    indexes.push_back((indexes_.find(itindex->second))->second);
  }
  return DB_SUCCESS;
 
}

dberr_t CatalogManager::DropTable(const string &table_name) { 
  /*first check if exist this table*/
  auto name_id = table_names_.find(table_name);
  if (name_id == table_names_.end()) {
    return DB_TABLE_NOT_EXIST;
  }
  /*drop all the relevant index*/
  auto tableName_index = index_names_.find(table_name);
  for (auto it = (tableName_index->second).begin(); it != (tableName_index->second).end(); it++) {
    /*it->first is the index name*/
    DropIndex(table_name, it->first);
  }

  /*drop the table，use table_heap */

}

dberr_t CatalogManager::DropIndex(const string &table_name, const string &index_name) {
  unordered_map<string, index_id_t> k;
  auto it = index_names_.find(table_name);
  if (it == index_names_.end()) return DB_FAILED;
  k = it->second;
  auto t = k.find(index_name);
  index_id_t a = t->second;
  IndexInfo *p = indexes_[a];
  p = NULL;
  indexes_.erase(a);
  k.erase(index_name);

}


dberr_t CatalogManager::FlushCatalogMetaPage() const {
  Page *p = buffer_pool_manager_->FetchPage(CATALOG_META_PAGE_ID);
  catalog_meta_->SerializeTo(p->GetData());
  buffer_pool_manager_->UnpinPage(CATALOG_META_PAGE_ID,true);
  return DB_SUCCESS;
  //CatalogManager(buffer_pool_manager_,lock_manager_,log_manager_,1);
}

dberr_t CatalogManager::LoadTable(const table_id_t table_id, const page_id_t page_id) {
  catalog_meta_->table_meta_pages_.insert(pair<table_id_t, page_id_t>(table_id, page_id));
  return DB_SUCCESS;
}

dberr_t CatalogManager::LoadIndex(const index_id_t index_id, const page_id_t page_id) {
  catalog_meta_->index_meta_pages_.insert(pair<index_id_t, page_id_t>(index_id, page_id));
  return DB_SUCCESS;
}

dberr_t CatalogManager::GetTable(const table_id_t table_id, TableInfo *&table_info) {
  /*auto it = tables_.find(table_id);
  table_info = it->second;
  return */


  //used in GetTable, not implemented
  return DB_SUCCESS;
}
