#include <iostream>
# include <cstring>
#include "Buffer/StaticBuffer.h"
#include "Cache/OpenRelTable.h"
#include "Disk_Class/Disk.h"
#include "FrontendInterface/FrontendInterface.h"
void updateAttributeName (const char* relName,const char* oldAttrName, const char* newAttrName) {
	// used to hold reference to the block which referred to 
	// for getting records, headers and updating them
	RecBuffer attrCatBuffer (ATTRCAT_BLOCK);
	
	HeadInfo attrCatHeader;
	attrCatBuffer.getHeader(&attrCatHeader);

	// iterating the records in the Attribute Catalog
	// to find the correct entry of relation and attribute
	for (int recIndex = 0; recIndex < attrCatHeader.numEntries; recIndex++) {
		Attribute attrCatRecord[ATTRCAT_NO_ATTRS];
		attrCatBuffer.getRecord(attrCatRecord, recIndex);

		// matching the relation name, and attribute name
		if (strcmp(attrCatRecord[ATTRCAT_REL_NAME_INDEX].sVal, relName) == 0
			&& strcmp(attrCatRecord[ATTRCAT_ATTR_NAME_INDEX].sVal, oldAttrName) == 0) 
		{
			strcpy(attrCatRecord[ATTRCAT_ATTR_NAME_INDEX].sVal, newAttrName);
			attrCatBuffer.setRecord(attrCatRecord, recIndex);
			std::cout << "Update successful!\n\n";
			break;
		}

		// reaching at the end of the block, and thus loading
		// the next block and setting the attrCatHeader & recIndex
		if (recIndex == attrCatHeader.numSlots-1) {
			recIndex = -1;
			attrCatBuffer = RecBuffer (attrCatHeader.rblock);
			attrCatBuffer.getHeader(&attrCatHeader);
		}
	}

}

void printTables()
{
// create objects for the relation catalog and attribute catalog
	RecBuffer relCatBuffer(RELCAT_BLOCK);
	RecBuffer attrCatBuffer(ATTRCAT_BLOCK);

	// creating headers for relation catalog and attribute catalog
	HeadInfo relCatHeader;
	HeadInfo attrCatHeader;

	// load the headers of both the blocks into relCatHeader and attrCatHeader.
	// (we will implement these functions later)
	relCatBuffer.getHeader(&relCatHeader);
	attrCatBuffer.getHeader(&attrCatHeader);
	// attrCatBaseSlot stores the index of current slot, 
	// which is incremented everytime an attribute entry 
	// is handled
	for (int i = 0, attrCatSlotIndex = 0; i < relCatHeader.numEntries; i++)
	{
		// will store the record from the relation catalog
		Attribute relCatRecord[RELCAT_NO_ATTRS]; 
		relCatBuffer.getRecord(relCatRecord, i);
		

		printf("Relation: %s\n", relCatRecord[RELCAT_REL_NAME_INDEX].sVal);

		int j = 0;
		for (; j < relCatRecord[RELCAT_NO_ATTRIBUTES_INDEX].nVal; j++, attrCatSlotIndex++)
		{
			// declare attrCatRecord and load the attribute catalog entry into it
			Attribute attrCatRecord[ATTRCAT_NO_ATTRS];
			attrCatBuffer.getRecord(attrCatRecord, attrCatSlotIndex);

			// if the current attribute belongs to the current relation
			// then we print it, which is checked by comparing names
			if (strcmp(attrCatRecord[ATTRCAT_REL_NAME_INDEX].sVal,
					   relCatRecord[RELCAT_REL_NAME_INDEX].sVal) == 0)
			{
				const char *attrType = attrCatRecord[ATTRCAT_ATTR_TYPE_INDEX].nVal == NUMBER
										   ? "NUM" : "STR";
				printf("  %s: %s\n", attrCatRecord[ATTRCAT_ATTR_NAME_INDEX].sVal, attrType);
			}

			// once all the slots are traversed, we update the block number
			// of the attrCatBuffer to the next block, and then attrCatHeader
			// is updated to the header of next block
			if (attrCatSlotIndex == attrCatHeader.numSlots-1) {
				attrCatSlotIndex = -1; // implementational operation, since the loop will increment anyways
				attrCatBuffer = RecBuffer (attrCatHeader.rblock);
				attrCatBuffer.getHeader(&attrCatHeader);
			}
		}

		printf("\n");
}
}
//int main(int argc, char *argv[]) {
  //Disk disk_run;
int main(int argc, char *argv[]) {
  Disk disk_run;
StaticBuffer buffer;
OpenRelTable cache;
 
  //      updateAttributeName("Students","Class","Batch"); //updates the name of the attribute
        for (int relId = 0; relId < 3; relId++) {
		//int relId = 2;
		
		RelCatEntry relCatBuffer;
		RelCacheTable::getRelCatEntry(relId, &relCatBuffer);
		printf ("Relation: %s\n", relCatBuffer.relName);
		
		for (int attrIndex = 0; attrIndex < relCatBuffer.numAttrs; attrIndex++) {
			AttrCatEntry attrCatBuffer;
			AttrCacheTable::getAttrCatEntry(relId, attrIndex, &attrCatBuffer);
			const char *attrType = attrCatBuffer.attrType == NUMBER ? "NUM" : "STR";
			printf ("    %s: %s\n", attrCatBuffer.attrName, attrType);
			
			}
		
		printf("\n");
	}


  return 0;

}
 // unsigned char buffer[BLOCK_SIZE];
 // Disk::readBlock(buffer, 0);
 // char message[] = "hello";
 // memcpy(buffer + 20, message, 6);
 // Disk::writeBlock(buffer, 7000);

 /* unsigned char buffer2[BLOCK_SIZE];
  char message2[6];
  Disk::readBlock(buffer2, 0);
  memcpy(message2, buffer2 + 20, 6);
  std::cout << message2;

  return 0;

  // StaticBuffer buffer;
  // OpenRelTable cache;

 // return FrontendInterface::handleFrontend(argc, argv);*/

