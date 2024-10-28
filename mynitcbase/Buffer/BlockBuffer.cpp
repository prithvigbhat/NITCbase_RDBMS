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
    unsigned char *bufferPtr;
    /* get the starting address of the buffer containing the block
       using loadBlockAndGetBufferPtr(&bufferPtr). */
int res=loadBlockAndGetBufferPtr(&bufferPtr);
    // if loadBlockAndGetBufferPtr(&bufferPtr) != SUCCESS
        // return the value returned by the call.
if(res!=SUCCESS)
{
return res;
}
   /* get the header of the block using the getHeader() function */
struct HeadInfo head;
this->getHeader(&head);
    // get number of attributes in the block.
int attrCount=head.numAttrs;
    // get the number of slots in the block.
int slotCount=head.numSlots;
    // if input slotNum is not in the permitted range return E_OUTOFBOUND.
if(slotNum<0||slotNum>=slotCount)
{
    return E_OUTOFBOUND;
    }
    /* offset bufferPtr to point to the beginning of the record at required
       slot. the block contains the header, the slotmap, followed by all
       the records. so, for example,
       record at slot x will be at bufferPtr + HEADER_SIZE + (x*recordSize)
       copy the record from `rec` to buffer using memcpy
       (hint: a record will be of size ATTR_SIZE * numAttrs)
    */
int recordSize=attrCount * ATTR_SIZE;
unsigned char *slotPointer=bufferPtr + HEADER_SIZE + slotCount + recordSize * slotNum;

memcpy(slotPointer, rec, recordSize);
    // update dirty bit using setDirtyBit()
        StaticBuffer:: setDirtyBit(this->blockNum);
    /* (the above function call should not fail since the block is already
       in buffer and the blockNum is valid. If the call does fail, there
       exists some other issue in the code) */
return SUCCESS;
    // return SUCCESS
}

/* NOTE: This function will NOT check if the block has been initialised as a
   record or an index block. It will copy whatever content is there in that
   disk block to the buffer.
   Also ensure that all the methods accessing and updating the block's data
   should call the loadBlockAndGetBufferPtr() function before the access or
   update is done. This is because the block might not be present in the
   buffer due to LRU buffer replacement. So, it will need to be bought back
   to the buffer before any operations can be done.
 */
 
int BlockBuffer::loadBlockAndGetBufferPtr(unsigned char ** buffPtr) {
    /* check whether the block is already present in the buffer
       using StaticBuffer.getBufferNum() */
    int bufferNum = StaticBuffer::getBufferNum(this->blockNum);
if(bufferNum!=E_BLOCKNOTINBUFFER)
{
  for(int i=0; i<BUFFER_CAPACITY;i++)
  {                                 // if present (!=E_BLOCKNOTINBUFFER),
                                    // set the timestamp of the corresponding buffer to 0 and increment the
   if(i==bufferNum){                // timestamps of all other occupied buffers in BufferMetaInfo.
   StaticBuffer::metainfo[i].timeStamp=0;
     }
     else {
     StaticBuffer::metainfo[i].timeStamp++;
     }
   }
 }
   // else
        // get a free buffer using StaticBuffer.getFreeBuffer()
else
   { 
   bufferNum=StaticBuffer::getFreeBuffer(this->blockNum);
    if(bufferNum==E_OUTOFBOUND)
    {
      return E_OUTOFBOUND;
      }
 // if the call returns E_OUTOFBOUND, return E_OUTOFBOUND here as
 // the blockNum is invalid
 // Read the block into the free buffer using readBlock()
Disk::readBlock(StaticBuffer::blocks[bufferNum], this->blockNum);
}
    // store the pointer to this buffer (blocks[bufferNum]) in *buffPtr
 *buffPtr= StaticBuffer::blocks[bufferNum];
 return SUCCESS;
    // return SUCCESS;
}


/* used to get the slotmap from a record block
NOTE: this function expects the caller to allocate memory for `*slotMap`
*/
int RecBuffer::getSlotMap(unsigned char *slotMap) {
  unsigned char *bufferPtr;

  // get the starting address of the buffer containing the block using loadBlockAndGetBufferPtr().
  int ret = loadBlockAndGetBufferPtr(&bufferPtr);
  if (ret != SUCCESS) {
    return ret;
  }

  struct HeadInfo head;
  // get the header of the block using getHeader() function
   BlockBuffer::getHeader(&head);
  int slotCount = head.numSlots;/* number of slots in block from header */

  // get a pointer to the beginning of the slotmap in memory by offsetting HEADER_SIZE
  unsigned char *slotMapInBuffer = bufferPtr + HEADER_SIZE;
   memcpy(slotMap, slotMapInBuffer, slotCount);
  // copy the values from `slotMapInBuffer` to `slotMap` (size is `slotCount`)

  return SUCCESS;
}

int compareAttrs(Attribute attr1, Attribute attr2, int attrType) {
	return attrType == NUMBER ? 
		(attr1.nVal < attr2.nVal ? -1 : (attr1.nVal > attr2.nVal ? 1 : 0)) : 
		strcmp(attr1.sVal, attr2.sVal) ;
}


