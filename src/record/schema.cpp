#include "record/schema.h"
#include "glog/logging.h"

uint32_t Schema::SerializeTo(char *buf) const {
  char *begin = buf;
  MACH_WRITE_UINT32(buf, SCHEMA_MAGIC_NUM);
  buf += sizeof(uint32_t);//magic number
  MACH_WRITE_UINT32(buf, columns_.size());
  buf += sizeof(uint32_t);//number of columns
  for (auto it=columns_.begin();it!=columns_.end();it++) {
    buf+=(*it)->SerializeTo(buf);
  }
  uint32_t offset = buf - begin;
  buf = begin;

  return offset;
}

uint32_t Schema::GetSerializedSize() const {
  uint32_t offset = sizeof(uint32_t)*2;//magic number+number of columns
  for (auto it=columns_.begin();it!=columns_.end();it++) {
    offset += (*it)->GetSerializedSize();
  }
  return offset;
}

uint32_t Schema::DeserializeFrom(char *buf, Schema *&schema, MemHeap *heap) {
  if (schema != nullptr) {
    LOG(WARNING) << "Pointer to schema is not null in column deserialize." << std::endl;
  }

  char *begin = buf;
  uint32_t magic_num = MACH_READ_UINT32(buf);

  buf += sizeof(uint32_t);
  if (magic_num != SCHEMA_MAGIC_NUM) {
    LOG(WARNING) << "MAGIC_NUM wrong in column Deserialize" << std::endl;
    buf = begin;
    return 0;
  }

  uint32_t size = MACH_READ_UINT32(buf);
  buf += sizeof(uint32_t);
  std::vector<Column *> columns(size,nullptr);
  for (auto it=columns.begin();it!=columns.end();it++) {
    buf+=Column::DeserializeFrom(buf, (*it), heap);
  }
  
  schema = ALLOC_P(heap, Schema)(columns);

  size_t offset = buf - begin;
  buf = begin;
  return offset;
}