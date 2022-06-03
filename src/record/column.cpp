#include"glog/logging.h"
#include "record/column.h"
Column::Column(std::string column_name, TypeId type, uint32_t index, bool nullable, bool unique)
        : name_(std::move(column_name)), type_(type), table_ind_(index),
          nullable_(nullable), unique_(unique) {
  ASSERT(type != TypeId::kTypeChar, "Wrong constructor for CHAR type.");
  switch (type) {
    case TypeId::kTypeInt :
      len_ = sizeof(int32_t);
      break;
    case TypeId::kTypeFloat :
      len_ = sizeof(float_t);
      break;
    default:
      ASSERT(false, "Unsupported column type.");
  }
}

Column::Column(std::string column_name, TypeId type, uint32_t length, uint32_t index, bool nullable, bool unique)
        : name_(std::move(column_name)), type_(type), len_(length),
          table_ind_(index), nullable_(nullable), unique_(unique) {
  ASSERT(type == TypeId::kTypeChar, "Wrong constructor for non-VARCHAR type.");
}

Column::Column(const Column *other) : name_(other->name_), type_(other->type_), len_(other->len_),
                                      table_ind_(other->table_ind_), nullable_(other->nullable_),
                                      unique_(other->unique_) {}

uint32_t Column::SerializeTo(char *buf) const {
  char *begin = buf;
  /*magic num*/
  MACH_WRITE_UINT32(buf, COLUMN_MAGIC_NUM);
  buf += sizeof(uint32_t);
  /*first: name_.length()+name, */
  MACH_WRITE_UINT32(buf, name_.length());
  buf += sizeof(uint32_t);
  MACH_WRITE_STRING(buf, name_);
  buf += (unsigned long)name_.length();
  /*TypeId*/
  MACH_WRITE_TO(TypeId, buf, type_);
  buf += sizeof(TypeId);
  /*len_*/
  MACH_WRITE_UINT32(buf, len_);
  buf += sizeof(uint32_t);
  /*table_index*/
  MACH_WRITE_UINT32(buf, table_ind_);
  buf += sizeof(uint32_t);
  /*nullable_and unique_*/
  MACH_WRITE_BOOL(buf,nullable_);
  buf += sizeof(bool);
  MACH_WRITE_BOOL(buf, unique_);
  buf += sizeof(bool);

  uint32_t offset = buf - begin;

  buf = begin;
  return offset;
}

uint32_t Column::GetSerializedSize() const {
  return sizeof(uint32_t) * 4 + (unsigned long)name_.length() + sizeof(TypeId) + sizeof(bool)*2;
}

uint32_t Column::DeserializeFrom(char *buf, Column *&column, MemHeap *heap) {
  if (column != nullptr) {
    LOG(WARNING) << "Pointer to column is not null in column deserialize." << std::endl;
  }
  char *begin = buf;
  uint32_t magic_num = MACH_READ_UINT32(buf);
  
  buf += sizeof(uint32_t);
  if (magic_num != COLUMN_MAGIC_NUM) {
    LOG(WARNING) << "MAGIC_NUM wrong in column Deserialize" << std::endl;
    buf = begin;
    return 0;
  }
  uint32_t name_length = MACH_READ_UINT32(buf);
  buf += sizeof(uint32_t);

  char* column_name=new char[name_length+1];
  MACH_READ_STR(column_name, buf, name_length);
  column_name[name_length] = 0;
  buf += name_length;

  TypeId type = MACH_READ_FROM(TypeId,buf);
  buf += sizeof(TypeId);

  uint32_t len = MACH_READ_UINT32(buf);
  buf += sizeof(uint32_t);
  uint32_t col_ind=MACH_READ_UINT32(buf);
  buf += sizeof(uint32_t);
  bool nullable=MACH_READ_BOOL(buf);
  buf += sizeof(bool);
  bool unique=MACH_READ_BOOL(buf);
  buf += sizeof(bool);
  if (type == kTypeChar) {
    column = ALLOC_P(heap, Column)(column_name, type, len, col_ind, nullable, unique);
  } else {
    column = ALLOC_P(heap, Column)(column_name, type, col_ind, nullable, unique);
  }
  size_t offset = buf - begin;
  buf = begin;
  delete[] column_name;
  return offset;
}
