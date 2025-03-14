/*
Oscar Zapata - 908 440 2404
Shamita
Jerry
*/

#include <memory.h>
#include <unistd.h>
#include <errno.h>
#include <stdlib.h>
#include <fcntl.h>
#include <iostream>
#include <stdio.h>
#include "page.h"
#include "buf.h"

#define ASSERT(c)  { if (!(c)) { \
		       cerr << "At line " << __LINE__ << ":" << endl << "  "; \
                       cerr << "This condition should hold: " #c << endl; \
                       exit(1); \
		     } \
                   }

//----------------------------------------
// Constructor of the class BufMgr
//----------------------------------------

BufMgr::BufMgr(const int bufs)
{
    numBufs = bufs;

    bufTable = new BufDesc[bufs];
    memset(bufTable, 0, bufs * sizeof(BufDesc));
    for (int i = 0; i < bufs; i++) 
    {
        bufTable[i].frameNo = i;
        bufTable[i].valid = false;
    }

    bufPool = new Page[bufs];
    memset(bufPool, 0, bufs * sizeof(Page));

    int htsize = ((((int) (bufs * 1.2))*2)/2)+1;
    hashTable = new BufHashTbl (htsize);  // allocate the buffer hash table

    clockHand = bufs - 1;
}


BufMgr::~BufMgr() {

    // flush out all unwritten pages
    for (int i = 0; i < numBufs; i++) 
    {
        BufDesc* tmpbuf = &bufTable[i];
        if (tmpbuf->valid == true && tmpbuf->dirty == true) {

#ifdef DEBUGBUF
            cout << "flushing page " << tmpbuf->pageNo
                 << " from frame " << i << endl;
#endif

            tmpbuf->file->writePage(tmpbuf->pageNo, &(bufPool[i]));
        }
    }

    delete [] bufTable;
    delete [] bufPool;
}

/*
 * [NOT GIVEN METHOD] This method finds a frame number to allocate into.
 * @param frame the frame number that is available for allocation
 * @return OK if no errors encountred,UNIXERR if problem encountred writing page, BUFFEREXCEEDED if no frames have pinCnt of 0
 */
const Status BufMgr::allocBuf(int & frame) 
{
    int iterations = 0;

    // search for unused frame
    while(iterations < numBufs * 2) {
        clockHand = (clockHand + 1) % numBufs;
        BufDesc &frameDesc = bufTable[clockHand];

        // if found invalid frame
        if(!frameDesc.valid) {
            frameDesc.Set(nullptr, -1);
            frame = clockHand;
            return OK;
        }

        if(frameDesc.pinCnt == 0) {
            // Found pin count 0 but refbit is true
            if(frameDesc.refbit) {
                frameDesc.refbit = 0;
            }
            else {
                // if dirty, write to disk
                if(frameDesc.dirty) {
                    Status status = frameDesc.file->writePage(frameDesc.pageNo,&bufPool[clockHand]);
                    if(status != OK) {
                        return UNIXERR;
                    }
                    frameDesc.dirty = 0;
                }

                hashTable->remove(frameDesc.file,frameDesc.pageNo);
                frameDesc.Set(nullptr, -1);
                frame = clockHand;
                return OK;
            }
        }
        iterations++;
    }
    return BUFFEREXCEEDED;
}

/*
 * [NOT GIVEN METHOD] This method reads a page from memory or I/O.
 * @param file the file this page belongs to
 * @param PageNo page number of this given page
 * @param page actual page object that has been read from storage
 * @return OK if no errors, UNIXERR if problem reading pge, HASHTBLERROR if problem inserting page, BUFFEREXCEEDED if no available frames
 */
const Status BufMgr::readPage(File* file, const int PageNo, Page*& page)
{
    int frameNo;
    Status status = hashTable->lookup(file,PageNo,frameNo);
    // Case 1 page not in buffer pool
    if(status == HASHNOTFOUND) {
        Status allocStatus = allocBuf(frameNo);
        if(allocStatus != OK) return allocStatus;

        //Read page from disk into the buffer pool frame
        Status readStatus = file->readPage(PageNo, &bufPool[frameNo]);
        if(readStatus != OK) return UNIXERR;
        
        // Insert page into hashtable
        Status insertStatus = hashTable->insert(file,PageNo,frameNo);
        if(insertStatus != OK) return HASHTBLERROR;

        bufTable[frameNo].Set(file,PageNo);
        bufTable[frameNo].pinCnt = 1;

        page = &bufPool[frameNo];

        return OK;
    }

    // Case 2 page is in buffer pool
    else {
        bufTable[frameNo].refbit = 1;
        bufTable[frameNo].pinCnt++;
        page = &bufPool[frameNo];

        return OK;
    }
}


/*
 * [NOT GIVEN METHOD] This method decrements pin count and returns status. 
 * @param file the file this page belongs to
 * @param PageNo page number of this given page
 * @param bool dirty status of corresponding frame
 * @return OK if no errors, PAGENOTPINNED if the pin count is already 0, HASHNOTFOUND if the page is not in the buffer pool hash table
*/
const Status BufMgr::unPinPage(File* file, const int PageNo, 
			       const bool dirty) 
{
    //Finds frame and checks corresponding status
    int frameNo = 0;
    Status returnStatus = hashTable->lookup(file, PageNo, frameNo);

    //Returns PAGENOTPINNED if the pin count is already 0.
    if (bufTable[frameNo].pinCnt == 0)
    {
        return PAGENOTPINNED;
    }

    //Returns HASHNOTFOUND if the page is not in the buffer pool hash table
    if (returnStatus == HASHNOTFOUND)
    {
        return HASHNOTFOUND
    }

    //Decrements the pinCnt of the frame containing (file, PageNo) and, if dirty == true, sets the dirty bit.
    bufTable[frameNo].pinCnt--;
    if (dirty == true)
    {
        bufTable[frame].dirty = true;
    }
    //Returns OK if no errors occurred
    return OK;
    
}

const Status BufMgr::allocPage(File* file, int& pageNo, Page*& page) 
{







}

const Status BufMgr::disposePage(File* file, const int pageNo) 
{
    // see if it is in the buffer pool
    Status status = OK;
    int frameNo = 0;
    status = hashTable->lookup(file, pageNo, frameNo);
    if (status == OK)
    {
        // clear the page
        bufTable[frameNo].Clear();
    }
    status = hashTable->remove(file, pageNo);

    // deallocate it in the file
    return file->disposePage(pageNo);
}

const Status BufMgr::flushFile(const File* file) 
{
  Status status;

  for (int i = 0; i < numBufs; i++) {
    BufDesc* tmpbuf = &(bufTable[i]);
    if (tmpbuf->valid == true && tmpbuf->file == file) {

      if (tmpbuf->pinCnt > 0)
	  return PAGEPINNED;

      if (tmpbuf->dirty == true) {
#ifdef DEBUGBUF
	cout << "flushing page " << tmpbuf->pageNo
             << " from frame " << i << endl;
#endif
	if ((status = tmpbuf->file->writePage(tmpbuf->pageNo,
					      &(bufPool[i]))) != OK)
	  return status;

	tmpbuf->dirty = false;
      }

      hashTable->remove(file,tmpbuf->pageNo);

      tmpbuf->file = NULL;
      tmpbuf->pageNo = -1;
      tmpbuf->valid = false;
    }

    else if (tmpbuf->valid == false && tmpbuf->file == file)
      return BADBUFFER;
  }
  
  return OK;
}


void BufMgr::printSelf(void) 
{
    BufDesc* tmpbuf;
  
    cout << endl << "Print buffer...\n";
    for (int i=0; i<numBufs; i++) {
        tmpbuf = &(bufTable[i]);
        cout << i << "\t" << (char*)(&bufPool[i]) 
             << "\tpinCnt: " << tmpbuf->pinCnt;
    
        if (tmpbuf->valid == true)
            cout << "\tvalid\n";
        cout << endl;
    };
}


