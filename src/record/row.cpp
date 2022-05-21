#include "record/row.h"
const unsigned char mask[8] = {0x80, 0x40, 0x20, 0x10, 0x08, 0x04, 0x02, 0x01};


uint32_t Row::SerializeTo(char *buf, Schema *schema) const {
  char *begin = buf;
  //MACH_WRITE_UINT32(buf, ROW_MAGIC_NUM);
  //buf += sizeof(uint32_t);  // magic number

  /*number of fields*/
  MACH_WRITE_UINT32(buf, fields_.size());
  buf += sizeof(uint32_t);
  /*bitmap for row*/

  uint32_t bitmap_size = (fields_.size() %8  == 0) ? fields_.size() / 8 : fields_.size() / 8 + 1;
  char* bitmap=new char[bitmap_size];
  memset(bitmap, 0, bitmap_size);//all initialize with 0
  uint32_t byte=0;
  uint32_t bit = 0;
  for (auto it=fields_.begin();it!=fields_.end();it++) {
      if ((*it)->IsNull() == false) {
      bitmap[byte] |= mask[bit];
    }
      bit++;
    if (bit == 8) {
      byte++;
      bit = 0;
    }
  }
  /*write bitmap*/
  for (uint32_t i = 0; i < bitmap_size; i++) {
    MACH_WRITE_CHAR(buf, bitmap[i]);
    buf += sizeof(char);
  }
  /*write each field. from the serialize function of field,we can find that if this
  field is null, we will do nothing.
  */
  for (auto it = fields_.begin();it!=fields_.end();it++) {
    buf += (*it)->SerializeTo(buf);
  }

  uint32_t offset = buf - begin;
  buf = begin;
  delete[] bitmap;
  return offset;
}

uint32_t Row::DeserializeFrom(char *buf, Schema *schema) {

  char *begin = buf;
  //uint32_t magic_num = MACH_READ_UINT32(buf);
  ///*check magic number*/
  //buf += sizeof(uint32_t);
  //if (magic_num != ROW_MAGIC_NUM) {
  //  LOG(WARNING) << "MAGIC_NUM wrong in row Deserialize" << std::endl;
  //  buf = begin;
  //  return 0;
  //}
  uint32_t size = MACH_READ_UINT32(buf);
  buf += sizeof(uint32_t);
  /*read in bitmap*/
  uint32_t bitmap_size = (size % 8 == 0) ? size / 8 : size / 8 + 1;
  char *bitmap = new char[bitmap_size];
  for (uint32_t i = 0; i < bitmap_size; i++) {
    bitmap[i] = MACH_READ_FROM(char, buf);
    buf += sizeof(char);
  }
  /*bitmap[byte]<<bit&0x80 to judge if this is null*/
  fields_.clear();

  uint32_t byte=0;
  uint32_t bit = 0;
  for (uint32_t i = 0; i < size; i++) {
    fields_.emplace_back(nullptr);
    if (((bitmap[byte] << bit) & mask[0] )!= 0) {
      /*this is not a null bit*/
      buf+=Field::DeserializeFrom(buf, schema->GetColumn(byte * 8 + bit)->GetType(), &fields_[byte * 8 + bit], false, heap_);
    } else {
      buf+=Field::DeserializeFrom(buf, schema->GetColumn(byte * 8 + bit)->GetType(), &fields_[byte * 8 + bit], true, heap_);
    }
    bit++;
    if (bit == 8) {
      byte++;
      bit = 0;
    }
  }

  uint32_t offset = buf - begin;
  buf = begin;
  delete[] bitmap;
  return offset;
}

uint32_t Row::GetSerializedSize(Schema *schema) const { 
  //uint32_t offset = sizeof(uint32_t) * ;//magic number+ size
  uint32_t offset = sizeof(uint32_t);//size
  uint32_t bitmap_size = (fields_.size() % 8 == 0) ? fields_.size() / 8 : fields_.size() / 8 + 1;
  offset += sizeof(char) * bitmap_size; /*bitmap size*/
  for (auto it=fields_.begin();it!=fields_.end();it++) {
    offset += (*it)->GetSerializedSize();
  }
  return offset;
}
