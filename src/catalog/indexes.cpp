#include "catalog/indexes.h"

IndexMetadata *IndexMetadata::Create(const index_id_t index_id, const string &index_name,
                                     const table_id_t table_id, const vector<uint32_t> &key_map,
                                     MemHeap *heap) {
  void *buf = heap->Allocate(sizeof(IndexMetadata));
  return new(buf)IndexMetadata(index_id, index_name, table_id, key_map);
}

uint32_t IndexMetadata::SerializeTo(char *buf) const {
  char *begin = buf;
  MACH_WRITE_UINT32(buf, INDEX_METADATA_MAGIC_NUM);
  buf += sizeof(uint32_t);
  MACH_WRITE_UINT32(buf, index_id_);
  buf += sizeof(uint32_t);
  MACH_WRITE_UINT32(buf, index_name_.length());
  buf += sizeof(uint32_t);
  MACH_WRITE_STRING(buf, index_name_);
  buf += (unsigned long)index_name_.length();
  MACH_WRITE_UINT32(buf, table_id_);
  buf += sizeof(uint32_t);

  MACH_WRITE_UINT32(buf, key_map_.size());
  buf += sizeof(uint32_t);
  
  uint32_t size = key_map_.size();

  for (uint32_t i = 0; i < size; i++) {
    MACH_WRITE_UINT32(buf, key_map_[i]);
    buf += sizeof(uint32_t);
  }
  uint32_t offset = buf - begin;
  buf = begin;
  return offset;
}

uint32_t IndexMetadata::GetSerializedSize() const {
    return sizeof(uint32_t) * (5+key_map_.size()) + (unsigned long)index_name_.length();
}

uint32_t IndexMetadata::DeserializeFrom(char *buf, IndexMetadata *&index_meta, MemHeap *heap) {
  if (index_meta != nullptr) {
    LOG(WARNING) << "Pointer to index_meta is not null in index_meta deserialize." << std::endl;
  }
  char *begin = buf;
  uint32_t magic_num = MACH_READ_UINT32(buf);
  buf += sizeof(uint32_t);
  if (magic_num != INDEX_METADATA_MAGIC_NUM) {
    LOG(WARNING) << "MAGIC_NUM wrong in index Deserialize" << std::endl;
    buf = begin;
    return 0;
  }
  //需要反序列化得到一下参数：index_id_（iid），index_name_（i_name）， table_id_（tid），key_map_（kp）
  uint32_t iid = MACH_READ_UINT32(buf);
  buf += sizeof(uint32_t);

  uint32_t name_length = MACH_READ_UINT32(buf);
  buf += sizeof(uint32_t);

  char *i_name = new char[name_length + 1];
  MACH_READ_STR(i_name, buf, name_length);
  i_name[name_length] = '\0';
  buf += name_length;

  uint32_t tid = MACH_READ_UINT32(buf);
  buf += sizeof(uint32_t);

  uint32_t len = MACH_READ_UINT32(buf);
  buf += sizeof(uint32_t);

  std::vector<uint32_t> kt;
  for (uint32_t i = 0; i < len; i++) {
    uint32_t a = MACH_READ_UINT32(buf);
    kt.push_back(a);
    buf += sizeof(uint32_t);
  }
  index_meta = Create(iid, i_name, tid, kt, heap);//构建元信息
  size_t offset = buf - begin;
  buf = begin;
  delete[] i_name;
  return offset;
}
