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
    table_id_t k1 = MACH_READ_UINT32(buf);
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
  return sizeof(uint32_t) * (3 + 2 * (index_meta_pages_.size() + table_meta_pages_.size()));
}

CatalogMeta::CatalogMeta() {}

CatalogManager::CatalogManager(BufferPoolManager *buffer_pool_manager, LockManager *lock_manager,
                               LogManager *log_manager, bool init)
    : buffer_pool_manager_(buffer_pool_manager),
      lock_manager_(lock_manager),
      log_manager_(log_manager),
      heap_(new SimpleMemHeap()) {
  if (init == 1) {  //如果初始化为0，则清空所有变量
    catalog_meta_ = NULL;
    next_table_id_ = -1;
    next_index_id_ = -1;
    table_names_.clear();
    tables_.clear();
    index_names_.clear();
    indexes_.clear();                                                //将各个元素清空初始化
    Page *p = buffer_pool_manager->FetchPage(CATALOG_META_PAGE_ID);  //找到catalog序列化存入的逻辑页
    catalog_meta_->SerializeTo(p->GetData());  //将他序列化的结果存到逻辑页的数据里面
  } else {
    Page *p = buffer_pool_manager->FetchPage(CATALOG_META_PAGE_ID);
    catalog_meta_ =
        catalog_meta_->DeserializeFrom(p->GetData(), heap_);  //找到那个页然后反序列化得到结果以便后续的进一步完成
  }
}

CatalogManager::~CatalogManager() { delete heap_; }

dberr_t CatalogManager::CreateTable(const string &table_name, TableSchema *schema, Transaction *txn,
                                    TableInfo *&table_info) {
  next_table_id_ = catalog_meta_->GetNextTableId() + 1;  //找到下一个的id
  //找到root_page_id
  page_id_t meta_page_id = INVALID_PAGE_ID;
  Page *meta_page = buffer_pool_manager_->NewPage(meta_page_id);  // meta data for this table
  if (meta_page == nullptr) return DB_FAILED;
  page_id_t root_page_id = INVALID_PAGE_ID;
  Page *root_page = buffer_pool_manager_->NewPage(root_page_id);  // content for this table
  if (root_page == nullptr) return DB_FAILED;
  /*create metadata for this table*/
  TableMetadata *table_meta = TableMetadata::Create(next_table_id_, table_name, root_page_id, schema, heap_);
  table_meta->SerializeTo(meta_page->GetData());  //序列化这个元信息

  /*insert table_id ,meta page id pair in catalog meta data*/
  LoadTable(next_table_id_, meta_page_id);

  FlushCatalogMetaPage();

  /*write the tableinfo*/
  table_info = TableInfo::Create(heap_);
  TableHeap *table_heap;
  table_heap = TableHeap::Create(buffer_pool_manager_, schema, txn, log_manager_, lock_manager_, heap_);
  table_info->Init(table_meta, table_heap);

  /*update the info in catalog manager*/
  table_names_.insert(pair<string, table_id_t>(table_name, next_table_id_));
  tables_.insert(pair<table_id_t, TableInfo *>(next_table_id_, table_info));
}

dberr_t CatalogManager::GetTable(const string &table_name, TableInfo *&table_info) {
  auto it = table_names_.find(table_name);
  if (it == table_names_.end()) return DB_FAILED;
  table_id_t table_id = it->second;
  auto it_tables = tables_.find(table_id);
  if (it_tables == tables_.end()) return DB_FAILED;
  table_info = it_tables->second;  //将结果存到table_info中返回
}

dberr_t CatalogManager::GetTables(vector<TableInfo *> &tables) const {
  for (auto it = tables_.begin(); it != tables_.end(); it++) tables.push_back(it->second);
}

dberr_t CatalogManager::CreateIndex(const std::string &table_name, const string &index_name,
                                    const std::vector<std::string> &index_keys, Transaction *txn,
                                    IndexInfo *&index_info) {
  /**/
  table_id_t table_id = table_names_[table_name];
  index_id_t next_index_id = catalog_meta_->GetNextIndexId() + 1;

  /*the same as create table : new a page for metadata and a page for index content */

  page_id_t meta_page_id = INVALID_PAGE_ID;
  Page *meta_page = buffer_pool_manager_->NewPage(meta_page_id);

  /*create a key_map for index constructor*/
  TableInfo *table_info = tables_[table_id];
  vector<uint32_t> key_map;
  uint32_t column_index = -1;
  for (auto it = index_keys.begin(); it != index_keys.end(); it++) {
    if (table_info->GetSchema()->GetColumnIndex(*it, column_index) == DB_SUCCESS) {
      key_map.push_back(column_index);
    } else {
      /*this index_keys doesn't exist*/
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

  if (it == index_names_.end()) {
    return DB_FAILED;
  }
  (it->second).insert(pair<string, index_id_t>(index_name, next_index_id_));
  indexes_.insert(pair<index_id_t, IndexInfo *>(next_index_id_, index_info));

  // page_id_t meta_page_id;
  // Page *k = buffer_pool_manager_->NewPage(page_id);
  ////进行key_map的转换
  // TableInfo *tinfo = tables_[t];
  // vector<uint32_t> key_map_;
  // key_map_.clear();
  // Schema *schema = tinfo->GetSchema();
  // vector<Column *> columns_ = schema->GetColumns();
  // uint32_t size1 = columns_.size();
  // uint32_t size2 = index_keys.size();
  // for (uint32_t i=0;i<size1;i++)
  //   for (uint32_t j = 0;j < size2; j++) {
  //     string a= index_keys[i];
  //     string b= columns_[j]->GetName();
  //     if (a == b) {
  //       key_map_.push_back(columns_[j]->GetTableInd());
  //       break;
  //     }
  //   }
  // IndexMetadata *p;
  // p = p->Create(next_index_id_, index_name, t, key_map_, heap_);
  // char *buf = NULL;
  // p->SerializeTo(buf);
  // uint32_t size = p->GetSerializedSize();
  // memcpy(k->GetData(), buf, size);
  // LoadIndex(next_index_id_, page_id);
  // FlushCatalogMetaPage();
  // index_info->Create(heap_);
  // index_info->Init(p, tables_[t], buffer_pool_manager_);
  // unordered_map<string, index_id_t> q;
  // auto it = index_names_.find(table_name);
  // q = it->second;
  // q.insert(pair<string, index_id_t>(index_name, next_index_id_));
  // indexes_.insert(pair<index_id_t, IndexInfo *>(next_index_id_, index_info));
}
dberr_t CatalogManager::GetIndex(const std::string &table_name, const std::string &index_name,
                                 IndexInfo *&index_info) const {
  unordered_map<string, index_id_t> k;
  auto it = index_names_.find(table_name);
  k = it->second;
  auto a = k.find(index_name);
  index_id_t index_id = a->second;
  auto b = indexes_.find(index_id);
  index_info = b->second;
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
}

dberr_t CatalogManager::DropTable(const string &table_name) {
  auto it = table_names_.find(table_name);
  if (it == table_names_.end()) return DB_FAILED;
  table_id_t table_id = it->second;
  TableInfo *p = tables_[table_id];
  p->~TableInfo();  //删除这个表信息指针
  tables_.erase(table_id);
  table_names_.erase(table_name);  //将存放的表格中的内容删掉
}

dberr_t CatalogManager::DropIndex(const string &table_name, const string &index_name) {
  unordered_map<string, index_id_t> k;
  auto it = index_names_.find(table_name);
  if (it == index_names_.end()) return DB_FAILED;
  k = it->second;
  auto t = k.find(index_name);
  if (t == k.end()) return DB_FAILED;
  index_id_t index_id = t->second;
  auto c = indexes_.find(index_id);
  if (c == indexes_.end()) return DB_FAILED;
  IndexInfo *p = c->second;
  p->~IndexInfo();
  indexes_.erase(index_id);
  (it->second).erase(index_name);
  k.erase(index_name);
}

dberr_t CatalogManager::FlushCatalogMetaPage() const {
  Page *p = buffer_pool_manager_->FetchPage(CATALOG_META_PAGE_ID);
  catalog_meta_->SerializeTo(p->GetData());
  CatalogManager(buffer_pool_manager_, lock_manager_, log_manager_, 1);
}

dberr_t CatalogManager::LoadTable(const table_id_t table_id, const page_id_t page_id) {
  catalog_meta_->table_meta_pages_.insert(pair<table_id_t, page_id_t>(table_id, page_id));
}

dberr_t CatalogManager::LoadIndex(const index_id_t index_id, const page_id_t page_id) {
  catalog_meta_->index_meta_pages_.insert(pair<index_id_t, page_id_t>(index_id, page_id));
}

dberr_t CatalogManager::GetTable(const table_id_t table_id, TableInfo *&table_info) {
  auto it = tables_.find(table_id);
  if (it == tables_.end()) return DB_FAILED;
  table_info = it->second;
}
