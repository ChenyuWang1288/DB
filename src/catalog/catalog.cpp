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
  CatalogMeta *catalog = NewInstance(heap);
  uint32_t size = MACH_READ_UINT32(buf);
  buf += sizeof(uint32_t);
  for (uint32_t i = 0; i < size; i++) {
    table_id_t k1=MACH_READ_UINT32(buf);
    buf += sizeof(uint32_t);
    page_id_t k2 = MACH_READ_UINT32(buf);
    buf += sizeof(uint32_t);
    catalog->table_meta_pages_.insert(map<table_id_t, page_id_t>::value_type(k1, k2));
  }
  size = MACH_READ_UINT32(buf);
  buf += sizeof(uint32_t);
  for (uint32_t i = 0; i < size; i++) {
    table_id_t k1 = MACH_READ_UINT32(buf);
    buf += sizeof(uint32_t);
    page_id_t k2 = MACH_READ_UINT32(buf);
    buf += sizeof(uint32_t);
    catalog->index_meta_pages_.insert(map<table_id_t, page_id_t>::value_type(k1, k2));
  }
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
  if (init == 0) {
    catalog_meta_ = NULL;
    next_table_id_ = -1;
    next_index_id_ = -1;
    table_names_.clear();
    tables_.clear();
    index_names_.clear();
    indexes_.clear();
    Page *p = buffer_pool_manager->FetchPage(CATALOG_META_PAGE_ID);
    catalog_meta_->SerializeTo(p->GetData());
  } else {
    Page *p = buffer_pool_manager->FetchPage(CATALOG_META_PAGE_ID);
    catalog_meta_ = catalog_meta_->DeserializeFrom(p->GetData(), heap_);
  }
}

CatalogManager::~CatalogManager() {
  delete heap_;
}

dberr_t CatalogManager::CreateTable(const string &table_name, TableSchema *schema,
                                    Transaction *txn, TableInfo *&table_info) {
  next_table_id_ = catalog_meta_->GetNextTableId() + 1;
  //找到root_page_id
  page_id_t page_id;
  Page *k = buffer_pool_manager_->NewPage(page_id);
  TableMetadata *t = t->Create(next_table_id_, table_name, page_id, schema, heap_);
  t->SerializeTo(k->GetData());
  LoadTable(next_table_id_, page_id);
  FlushCatalogMetaPage();
  table_info->Create(heap_);
  TableHeap *p;
  p = p->Create(buffer_pool_manager_, schema, txn, log_manager_, lock_manager_, heap_);
  table_info->Init(t,p);
  table_names_.insert(pair<string, table_id_t>(table_name, next_table_id_));
  tables_.insert(pair<table_id_t, TableInfo *>(next_table_id_, table_info));
}

dberr_t CatalogManager::GetTable(const string &table_name, TableInfo *&table_info) { 
    table_id_t t;
  t = table_names_[table_name];
    table_info = tables_[t];
}

dberr_t CatalogManager::GetTables(vector<TableInfo *> &tables) const {
  auto it = tables_.begin();
  for ( it = tables_.begin(); it != tables_.end(); it++) {
    tables.push_back(it->second);
  }
  tables.push_back(it->second);
}

dberr_t CatalogManager::CreateIndex(const std::string &table_name, const string &index_name,
                                    const std::vector<std::string> &index_keys, Transaction *txn,
                                    IndexInfo *&index_info) {
  table_id_t t = table_names_[table_name];
  index_id_t next_index_id_ = catalog_meta_->GetNextIndexId() + 1;
  page_id_t page_id;
  Page *k = buffer_pool_manager_->NewPage(page_id);
  //进行key_map的转换
  IndexMetadata *p;
  p = p->Create(next_index_id_, index_name, t, key_map, heap_);
  char *buf = NULL;
  p->SerializeTo(buf);
  uint32_t size = p->GetSerializedSize();
  memcpy(k->GetData(), buf, size);
  LoadIndex(next_index_id_, page_id);
  FlushCatalogMetaPage();
  index_info->Create(heap_);
  index_info->Init(p, tables_[t], buffer_pool_manager_);
  unordered_map<string, index_id_t> q;
  auto it = index_names_.find(table_name);
  q = it->second;
  q.insert(pair<string, index_id_t>(index_name, next_index_id_));
  indexes_.insert(pair<index_id_t, IndexInfo *>(next_index_id_, index_info));
}
dberr_t CatalogManager::GetIndex(const std::string &table_name, const std::string &index_name,
                                 IndexInfo *&index_info) const {
  unordered_map<string, index_id_t> k ;
  auto it = index_names_.find(table_name);
  k = it->second;
}

dberr_t CatalogManager::GetTableIndexes(const std::string &table_name, std::vector<IndexInfo *> &indexes) const {
  unordered_map<string, index_id_t> k;
  auto it = index_names_.find(table_name);
  k = it->second;
  auto i = k.begin();
  for (i = k.begin(); i != k.end(); i++) {
    index_id_t t;
    t = i->second;
    auto p = indexes_.find(t);
    indexes.push_back(p->second);
  }
  index_id_t t;
  t = i->second;
  auto p = indexes_.find(t);
  indexes.push_back(p->second);
}

dberr_t CatalogManager::DropTable(const string &table_name) { 
    table_id_t t;
  t = table_names_[table_name];
    TableInfo *p = tables_[t];
  p = NULL;
    tables_.erase(t);
  table_names_.erase(table_name);
}

dberr_t CatalogManager::DropIndex(const string &table_name, const string &index_name) {
  unordered_map<string, index_id_t> k;
  auto it = index_names_.find(table_name);
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
  CatalogManager(buffer_pool_manager_,lock_manager_,log_manager_,1);
}

dberr_t CatalogManager::LoadTable(const table_id_t table_id, const page_id_t page_id) {
  catalog_meta_->table_meta_pages_.insert(pair<table_id_t, page_id_t>(table_id, page_id));
}

dberr_t CatalogManager::LoadIndex(const index_id_t index_id, const page_id_t page_id) {
  catalog_meta_->index_meta_pages_.insert(pair<index_id_t, page_id_t>(index_id, page_id));
}

dberr_t CatalogManager::GetTable(const table_id_t table_id, TableInfo *&table_info) {
  auto it = tables_.find(table_id);
  table_info = it->second;
}
