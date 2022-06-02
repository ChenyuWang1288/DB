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
  MACH_WRITE_TO(Schema, buf, *schema_);
  buf += sizeof(Schema);
  uint32_t offset = buf - begin;
  buf = begin;
  return offset;
}

uint32_t TableMetadata::GetSerializedSize() const {
  return sizeof(uint32_t) * 4 + (unsigned long)table_name_.length() + sizeof(Schema) ;
}

/**
 * @param heap Memory heap passed by TableInfo
 */
uint32_t TableMetadata::DeserializeFrom(char *buf, TableMetadata *&table_meta, MemHeap *heap) {
  if (table_meta != nullptr) {
    LOG(WARNING) << "Pointer to table_meta is not null in table_meta deserialize." << std::endl;
  }
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

  Schema *s;
  *s=MACH_READ_FROM(Schema, buf);
  buf += sizeof(Schema);

  table_meta = Create(tid, t_name, rid, s, heap);//创建元信息（所有的这里的类的信息都是由自己的heap创建的）
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
TableMetadata *TableMetadata::Create(table_id_t table_id, std::string table_name,
                                     page_id_t root_page_id, TableSchema *schema, MemHeap *heap) {
  // allocate space for table metadata
  void *buf = heap->Allocate(sizeof(TableMetadata));
  return new(buf)TableMetadata(table_id, table_name, root_page_id, schema);
}

TableMetadata::TableMetadata(table_id_t table_id, std::string table_name, page_id_t root_page_id, TableSchema *schema)
        : table_id_(table_id), table_name_(table_name), root_page_id_(root_page_id), schema_(schema) {}
