#include "BlockBuffer.h"

#include <cstdlib>
#include <cstring>
// the declarations for these functions can be found in "BlockBuffer.h"

BlockBuffer::BlockBuffer(int blockNum) {
  // initialise this.blockNum with the argument
this->blockNum=blockNum;
}

// calls the parent class constructor
RecBuffer::RecBuffer(int blockNum) : BlockBuffer::BlockBuffer(blockNum) {}

// load the block header into the argument pointer
int BlockBuffer::getHeader(struct HeadInfo *head) {
unsigned char *bufferPtr;
int ret=loadBlockAndGetBufferPtr(&bufferPtr);//we give the pointer instead of a disk block 
if(ret!=SUCCESS){
return ret;//return  the error if any
}
//unsigned char buffer[BLOCK_SIZE];

  // read the block at this.blockNum into the buffer
//Disk::readBlock(buffer,this->blockNum);
  // populate the numEntries, numAttrs and numSlots fields in *head
  //memcpy(&head->numSlots, buffer + 24, 4);
  memcpy(&head->numSlots,bufferPtr + 24,4);
  memcpy(&head->numEntries, bufferPtr + 16, 4);
  memcpy(&head->numAttrs ,bufferPtr + 20, 4);
  memcpy(&head->rblock, bufferPtr + 12, 4);
  memcpy(&head->lblock, bufferPtr+  8, 4);

  return SUCCESS;//"Stage 2 code"
}

// load the record at slotNum into the argument pointer
int RecBuffer::getRecord(union Attribute *rec, int slotNum) {
  struct HeadInfo head;

  // get the header using this.getHeader() function
  this->getHeader(&head);
  int attrCount = head.numAttrs;
  int slotCount = head.numSlots;

  // read the block at this.blockNum into a buffer
unsigned char *bufferPtr;
int ret=loadBlockAndGetBufferPtr(&bufferPtr);
if(ret!=SUCCESS){
return ret;
}
//unsigned char buffer[BLOCK_SIZE];
//Disk::readBlock(buffer,this->blockNum);
  /* record at slotNum will be at offset HEADER_SIZE + slotMapSize + (recordSize * slotNum)
     - each record will have size attrCount * ATTR_SIZE
     - slotMap will be of size slotCount
  */
  int recordSize = attrCount * ATTR_SIZE;
  unsigned char *slotPointer =bufferPtr +HEADER_SIZE + slotCount +(recordSize*slotNum) /* calculate buffer + offset */;

  // load the record into the rec data structure
  memcpy(rec, slotPointer, recordSize);

  return SUCCESS;
}

int RecBuffer::setRecord(union Attribute *rec, int slotNum)
{
  struct HeadInfo head;

  // get the header using this.getHeader() function
  this->getHeader(&head);

  int attrCount = head.numAttrs;
  int slotCount = head.numSlots;

  // read the block at this.blockNum into a buffer
  unsigned char buffer[BLOCK_SIZE];
  Disk::readBlock(buffer, this->blockNum);

  /* record at slotNum will be at offset HEADER_SIZE + slotMapSize + (recordSize * slotNum)
     - each record will have size attrCount * ATTR_SIZE
     - slotMap will be of size slotCount
  */
  int recordSize = attrCount * ATTR_SIZE;
  unsigned char *slotPointer = buffer + 32 + slotCount + recordSize * slotNum;

  // load the record into the rec data structure
  memcpy(slotPointer, rec, recordSize);
  Disk::writeBlock(buffer,this->blockNum);

  return SUCCESS;
}

int BlockBuffer::loadBlockAndGetBufferPtr(unsigned char **buffPtr) {
  // check whether the block is already present in the buffer using StaticBuffer.getBufferNum()
  int bufferNum = StaticBuffer::getBufferNum(this->blockNum);

  if (bufferNum == E_BLOCKNOTINBUFFER) {
    bufferNum = StaticBuffer::getFreeBuffer(this->blockNum);

    if (bufferNum == E_OUTOFBOUND) {
      return E_OUTOFBOUND;
    }

    Disk::readBlock(StaticBuffer::blocks[bufferNum], this->blockNum);
  }

  // store the pointer to this buffer (blocks[bufferNum]) in *buffPtr
  *buffPtr = StaticBuffer::blocks[bufferNum];

  return SUCCESS;
}
