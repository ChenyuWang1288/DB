#include "catalog/table.h"

uint32_t TableMetadata::SerializeTo(char *buf) const {
  char *begin = buf;
  MACH_WRITE_UINT32(buf, TABLE_METADATA_MAGIC_NUM);
  buf += sizeof(uint32_t);
  MACH_WRITE_UINT32(buf, table_id_);
  buf += sizeof(uint32_t);
  MACH_WRITE_UINT32(buf, table_name_.length());
  buf += sizeof(uint32_t);
  MACH_WRITE_STRING(buf, table_name_);
  buf += (unsigned long)table_name_.length();
  MACH_WRITE_UINT32(buf, root_page_id_);
  buf += sizeof(uint32_t);
  /*schema */
  buf += schema_->SerializeTo(buf);//在schema中序列化
 
  MACH_WRITE_UINT32(buf, primarykey.size());
  buf += sizeof(uint32_t);
  for (auto it = primarykey.begin(); it != primarykey.end(); it++) {
    buf += it->SerializeTo(buf);//在colomn中序列化
  }

  uint32_t offset = buf - begin;
  buf = begin;
  return offset;
}

uint32_t TableMetadata::GetSerializedSize() const {
  uint32_t size = 0;
  for (auto it = primarykey.begin(); it != primarykey.end(); it++) {
    size += it->GetSerializedSize(); 
  }
  return sizeof(uint32_t) * 4 + (unsigned long)table_name_.length() + schema_->GetSerializedSize()+size;
}

/**
 * @param heap Memory heap passed by TableInfo
 */
uint32_t TableMetadata::DeserializeFrom(char *buf, TableMetadata *&table_meta, MemHeap *heap) {
  char *begin = buf;
  uint32_t magic_num = MACH_READ_UINT32(buf);

  buf += sizeof(uint32_t);
  if (magic_num != TABLE_METADATA_MAGIC_NUM) {
    LOG(WARNING) << "MAGIC_NUM wrong in table_meta Deserialize" << std::endl;
    buf = begin;//不改变buf
    return 0;//返回为0则出错了
  }
  //反序列化要得到的参数： table_id_（tid），table_name_（t_name），root_page_id_（rid），schema_（s）
  uint32_t tid = MACH_READ_UINT32(buf);
  buf += sizeof(uint32_t);
 
  uint32_t name_length = MACH_READ_UINT32(buf);
  buf += sizeof(uint32_t);

  char *t_name = new char[name_length + 1];
  MACH_READ_STR(t_name, buf, name_length);
  t_name[name_length] = '\0';
  buf += name_length;

  uint32_t rid = MACH_READ_UINT32(buf);
  buf += sizeof(uint32_t);

  /*deserialize schema*/
  Schema *s = nullptr;
  buf += Schema::DeserializeFrom(buf, s, heap);
  
  //deserialize primarykey
  uint32_t size = MACH_READ_UINT32(buf);
  buf += sizeof(uint32_t);
  std::vector<Column> pk;
  pk.clear();
  for (uint32_t i = 0; i < size; i++) {
    Column *primary__key = nullptr;
    buf += Column::DeserializeFrom(buf, primary__key, heap);
    pk.push_back(primary__key);
  }

  table_meta = Create(tid, t_name, rid, s, pk, heap);//创建元信息（所有的这里的类的信息都是由自己的heap创建的）
  size_t offset = buf - begin;
  buf = begin;//不更改buf的值
  delete[] t_name;
  return offset;
}

/**
 * Only called by create table
 *
 * @param heap Memory heap passed by TableInfo
 */
TableMetadata *TableMetadata::Create(table_id_t table_id, std::string table_name, page_id_t root_page_id,
                                     TableSchema *schema, vector<Column> primary_key, MemHeap *heap) {
  // allocate space for table metadata
  void *buf = heap->Allocate(sizeof(TableMetadata));
  return new (buf) TableMetadata(table_id, table_name, root_page_id, schema, primary_key);
}

TableMetadata::TableMetadata(table_id_t table_id, std::string table_name, page_id_t root_page_id, TableSchema *schema,
                             vector<Column> primary_key)
    : table_id_(table_id),
      table_name_(table_name),
      root_page_id_(root_page_id),
      schema_(schema),
      primarykey(primary_key) {}
