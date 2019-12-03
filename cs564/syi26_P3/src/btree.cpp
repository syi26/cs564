/**
 * @author See Contributors.txt for code contributors and overview of BadgerDB.
 *
 * @section LICENSE
 * Copyright (c) 2012 Database Group, Computer Sciences Department, University of Wisconsin-Madison.
 */

#include "btree.h"
#include "filescan.h"
#include "exceptions/bad_index_info_exception.h"
#include "exceptions/bad_opcodes_exception.h"
#include "exceptions/bad_scanrange_exception.h"
#include "exceptions/no_such_key_found_exception.h"
#include "exceptions/scan_not_initialized_exception.h"
#include "exceptions/index_scan_completed_exception.h"
#include "exceptions/file_not_found_exception.h"
#include "exceptions/end_of_file_exception.h"
#include "exceptions/hash_not_found_exception.h"
#include <iostream>
#include <fstream>
#include <cstdio>
#include <vector>
#include <string>
#include <algorithm>


//#define DEBUG
using namespace std;

namespace badgerdb {
// structure
IndexMetaInfo metadata;
// comparators
static auto compare_rid =
  [](const RecordId &rid_1, const RecordId &rid_2)
  {
    return rid_1.page_number > rid_2.page_number;
  };
static auto compare_pid =
  [](const PageId &pid_1, const PageId &pid_2)
  {
    return pid_1 > pid_2;
  };

// These two methods are for calculation node lengths
const int BTreeIndex::length_of_leaf(LeafNodeInt *leaf) {
    static RecordId rec;
    RecordId *first = leaf->ridArray;
    RecordId *last = &leaf->ridArray[this->leafOccupancy];
    RecordId *val = lower_bound(first, last, rec, compare_rid);
    int length = val - first;
    return length;
}

const int BTreeIndex::length_of_nonleaf(NonLeafNodeInt *nonleaf) {
    static int zero = 0;
    PageId *first = nonleaf->pageNoArray;
    PageId *last = &nonleaf->pageNoArray[this->nodeOccupancy + 1];
    PageId *val = lower_bound(first, last, zero, compare_pid);
    int length = val - first;
    return length;
}

// -----------------------------------------------------------------------------
// BTreeIndex::BTreeIndex -- Constructor
// -----------------------------------------------------------------------------

BTreeIndex::BTreeIndex(const string &relationName, string &outIndexName,
                       BufMgr *bufMgrIn, const int attrByteOffset,
                       const Datatype attrType) {
  // setting global variables
  try {
      this->bufMgr = bufMgrIn;
      this->attrByteOffset = attrByteOffset;
      this->attributeType = attrType;
      this->leafOccupancy = INTARRAYLEAFSIZE;
      this->nodeOccupancy = INTARRAYNONLEAFSIZE;
      this->scanExecuting = false;

      ostringstream idx_str;
      idx_str << relationName << ',' << this->attrByteOffset;
      outIndexName = idx_str.str();

      Page *headerPage;
      Page *rootPage;

      // check if file exists or not
      if (BlobFile::exists(outIndexName)) {
          // setting global variables
          this->file = new BlobFile(outIndexName, false);
          this->headerPageNum = this->file->getFirstPageNo();
          this->bufMgr->readPage(this->file, this->headerPageNum, headerPage);
          this->rootPageNum = metadata.rootPageNo;
          this->bufMgr->unPinPage(this->file, this->headerPageNum, false);
          if (relationName != metadata.relationName or
              this->attributeType != metadata.attrType or
              this->attrByteOffset != metadata.attrByteOffset)
              throw BadIndexInfoException(outIndexName);
      } else {
          // setting global variables
          this->file = new BlobFile(outIndexName, true);
          this->headerPageNum = this->file->getFirstPageNo();
          this->rootPageNum = metadata.rootPageNo;
          this->bufMgr->allocPage(this->file, this->headerPageNum, headerPage);
          this->bufMgr->allocPage(this->file, this->rootPageNum, rootPage);
          this->bufMgr->unPinPage(this->file, this->headerPageNum, true);
          this->bufMgr->unPinPage(this->file, this->rootPageNum, true);
          LeafNodeInt *leaf;
          this->bufMgr->allocPage(this->file, metadata.rootPageNo, (Page *&) leaf);
          memset(leaf, 0, Page::SIZE);
          leaf->level = -1;
          this->bufMgr->unPinPage(this->file, metadata.rootPageNo, true);
          // copy relation name
          strncpy(metadata.relationName, relationName.c_str(), sizeof(metadata.relationName) - 1);
          metadata.relationName[19] = '\0';
          metadata.attrByteOffset = this->attrByteOffset;
          metadata.attrType = this->attributeType;

          // scan file
          FileScan fs(relationName, this->bufMgr);
          try {
              RecordId rid;
              while (true) {
                  fs.scanNext(rid);
                  string rec = fs.getRecord();
                  insertEntry((void *) (rec.c_str() + this->attrByteOffset), rid);
              }
          } catch (EndOfFileException e) {
              // when reaches end of file, flush it
              this->bufMgr->flushFile(this->file);
          }
      }
  } catch (FileNotFoundException e) {
  }
}

// -----------------------------------------------------------------------------
// BTreeIndex::~BTreeIndex -- destructor
// -----------------------------------------------------------------------------

BTreeIndex::~BTreeIndex() {
    if (this->scanExecuting) endScan();
    this->scanExecuting = false;
    this->bufMgr->flushFile(this->file);
    // delete file
    delete this->file;
    this->file = nullptr;
}

// get the position in an array
const int BTreeIndex::idx_in_array(const int *array, int idx, int key) {
    return lower_bound(array, &array[idx], key) - array;
}

// method to insert leaf node
const void BTreeIndex::insert_leaf(LeafNodeInt *leaf, int idx, int key, RecordId rid) {
  size_t length = this->leafOccupancy - idx;
  // copy memory of former node
  memcpy(&leaf->keyArray[idx], &leaf->keyArray[idx - 1], length * 4);
  memcpy(&leaf->ridArray[idx], &leaf->ridArray[idx - 1], length * sizeof(RecordId));
  // save values
  leaf->keyArray[idx - 1] = key;
  leaf->ridArray[idx - 1] = rid;
}

// method to insert nonleaf node
const void BTreeIndex::insert_nonleaf(NonLeafNodeInt *nonleaf, int idx, int key, PageId pid) {
  size_t length = this->nodeOccupancy - idx;
  // copy memory of former node
  memcpy(&nonleaf->keyArray[idx], &nonleaf->keyArray[idx - 1], length * 4);
  memcpy(&nonleaf->pageNoArray[idx + 1], &nonleaf->pageNoArray[idx], length * sizeof(PageId));
  // save values
  nonleaf->keyArray[idx - 1] = key;
  nonleaf->pageNoArray[idx] = pid;
}

PageId BTreeIndex::insert(PageId former_pid, int key, RecordId rid, int &mid_val) {
  Page *former_page;
  // read former page
  this->bufMgr->readPage(this->file, former_pid, former_page);
  // check if it is leaf
  if (*(int*)former_page == -1)  {
      LeafNodeInt *former_leaf = (LeafNodeInt*)former_page;
      // find index of former leaf
      int leaf_length = length_of_leaf(former_leaf);
      int idx_temp = idx_in_array(former_leaf->keyArray, leaf_length, key);
      int index = (idx_temp >= leaf_length) ? leaf_length : idx_temp;

      // check if leaf is empty
      if (former_leaf->ridArray[this->leafOccupancy - 1].page_number == 0 and
          former_leaf->ridArray[this->leafOccupancy - 1].slot_number == 0) {
          insert_leaf(former_leaf, index + 1, key, rid);
          // unpin page after insertion
          this->bufMgr->unPinPage(this->file, former_pid, true);
          return 0;
      }
      // get the middle index
      const int mid_idx = this->leafOccupancy / 2;
      PageId new_pid;
      LeafNodeInt *new_leaf;
      // alloc page for new node
      this->bufMgr->allocPage(this->file, new_pid, (Page*&)new_leaf);
      new_leaf->level = -1;

      // check where to insert the leaf
      if (index < mid_idx) {
          size_t length = this->leafOccupancy - (mid_idx + 1);
          // copy the former node info to new node
          memcpy(&new_leaf->keyArray, &former_leaf->keyArray[mid_idx + 1], length * sizeof(int));
          memcpy(&new_leaf->ridArray, &former_leaf->ridArray[mid_idx + 1], length * sizeof(RecordId));
          // clear info
          memset(&former_leaf->ridArray[mid_idx + 1], 0, length * sizeof(RecordId));
          // insert leaf node to right
          insert_leaf(former_leaf, index + 1, key, rid);
      }
      else {
          size_t length = this->leafOccupancy - (mid_idx);
          // copy the former node info to new node
          memcpy(&new_leaf->keyArray, &former_leaf->keyArray[mid_idx], length * sizeof(int));
          memcpy(&new_leaf->ridArray, &former_leaf->ridArray[mid_idx], length * sizeof(RecordId));
          //clear info
          memset(&former_leaf->ridArray[mid_idx], 0, length * sizeof(RecordId));
          // insert leaf node to left
          insert_leaf(new_leaf, index + 1 - mid_idx, key, rid);
      }

      new_leaf->rightSibPageNo = former_leaf->rightSibPageNo;
      former_leaf->rightSibPageNo = new_pid;
      // unpin former node page and new node page
      this->bufMgr->unPinPage(this->file, former_pid, true);
      this->bufMgr->unPinPage(this->file, new_pid, true);
      // update middle value
      mid_val = new_leaf->keyArray[0];
      return new_pid;
  }

  // if a nonleaf should be inserted instead
  NonLeafNodeInt *former_nonleaf = (NonLeafNodeInt*)former_page;

  // find index of former nonleaf node
  int length = length_of_nonleaf(former_nonleaf);
  int idx_temp2 = idx_in_array(former_nonleaf->keyArray, length - 1, key);
  int former_idx = (idx_temp2 >= length - 1) ? (length - 1) : idx_temp2;
  PageId former_pid2 = former_nonleaf->pageNoArray[former_idx];

  int new_mid_val;
  // get the page id for new node
  PageId new_pid = insert(former_pid2, key, rid, new_mid_val);

  // does not split
  if (!new_pid) {
    this->bufMgr->unPinPage(this->file, former_pid, false);
    return 0;
  }

  // split
  int index = (idx_temp2 >= length) ? length : idx_temp2;
  // find where to insert nonleaf node
  if (this->nodeOccupancy == 0) {
      if (former_nonleaf->pageNoArray[1] > 0) {
          insert_nonleaf(former_nonleaf, index + 1, new_mid_val, new_pid);
          this->bufMgr->unPinPage(this->file, former_pid, true);
          return 0;
      }
  } else {
      if (former_nonleaf->pageNoArray[0] > 0) {
          insert_nonleaf(former_nonleaf, index + 1, new_mid_val, new_pid);
          this->bufMgr->unPinPage(this->file, former_pid, true);
          return 0;
      }
  }

  // find middle index
  int mid_idx = (int)((double)(this->nodeOccupancy - 1) / 2);

  // find split index
  int split = (index < mid_idx) ? (mid_idx + 1) : mid_idx;
  // find the insert index
  int idx = (index < mid_idx) ? index : (index - mid_idx);

  PageId new_pid2;
  NonLeafNodeInt *new_nonleaf;
  // allocate new node page
  this->bufMgr->allocPage(this->file, new_pid2, (Page *&)new_nonleaf);
  size_t length_idx = this->nodeOccupancy - split;

  // check to insert to right or left
  if (index >= mid_idx and idx == 0) {
      // insert to right
      mid_val = new_mid_val;
      // copy memory of former node
      memcpy(&new_nonleaf->keyArray, &former_nonleaf->keyArray[split], length_idx * sizeof(int));
      memcpy(&new_nonleaf->pageNoArray, &former_nonleaf->pageNoArray[split + 1], length_idx * sizeof(PageId));
  } else {
      // insert to left
      mid_val = former_nonleaf->keyArray[split];
      // copy memory of former node
      memcpy(&new_nonleaf->keyArray, &former_nonleaf->keyArray[split + 1], (length_idx - 1) * sizeof(int));
      memcpy(&new_nonleaf->pageNoArray, &former_nonleaf->pageNoArray[split + 1], length_idx * sizeof(PageId));
      NonLeafNodeInt *result_nonleaf = (index < mid_idx) ? former_nonleaf : new_nonleaf;
      insert_nonleaf(result_nonleaf, idx + 1, new_mid_val, new_pid2);
  }
  // unpin former and new pages
  this->bufMgr->unPinPage(this->file, former_pid, true);
  this->bufMgr->unPinPage(this->file, new_pid2, true);

  // return page id of the new page
  return new_pid2;
}

// -----------------------------------------------------------------------------
// BTreeIndex::insertEntry
// -----------------------------------------------------------------------------

const void BTreeIndex::insertEntry(const void *key, const RecordId rid) {
  // default 0 for middle value at initialization
  int mid_val = 0;
  // find pid for root page
  PageId pid = insert(metadata.rootPageNo, *(int *)key, rid, mid_val);

  if (pid == 0) {
      return;
  } else {
      PageId new_root_pid;
      NonLeafNodeInt *root;
      // allocate page for root node
      this->bufMgr->allocPage(this->file, new_root_pid, (Page *&)root);
      // update root values
      root->keyArray[0] = mid_val;
      root->pageNoArray[0] = metadata.rootPageNo;
      root->pageNoArray[1] = pid;
      // unpin the root page
      this->bufMgr->unPinPage(this->file, new_root_pid, true);
      metadata.rootPageNo = new_root_pid;
  }
}

// -----------------------------------------------------------------------------
// BTreeIndex::startScan
// -----------------------------------------------------------------------------

const void BTreeIndex::startScan(const void *lowValParm,
        const Operator lowOpParm,
        const void *highValParm,
        const Operator highOpParm) {

        // check if there is an existing scan
        this->scanExecuting = true;
        // check value for low
        if (lowOpParm != GT and lowOpParm != GTE)
            throw BadOpcodesException();
        else {
            this->lowOp = lowOpParm;
            this->lowValInt = *(int *) lowValParm;
        }
        // check value for high
        if (highOpParm != LT and highOpParm != LTE)
            throw BadOpcodesException();
        else {
            this->highOp = highOpParm;
            this->highValInt = *(int *) highValParm;
        }
        // check if low value is higher than high value
        if (this->lowValInt > this->highValInt) throw BadScanrangeException();
        this->currentPageNum = metadata.rootPageNo;

        while (true) {
            // read next page
            this->bufMgr->readPage(this->file, this->currentPageNum, this->currentPageData);
            // if it's leaf, break loop
            if (*(int *) this->currentPageData == -1) break;
            NonLeafNodeInt *nonleaf = (NonLeafNodeInt *) this->currentPageData;
            // unpin current page
            this->bufMgr->unPinPage(this->file, this->currentPageNum, false);

            // find the length of nonleaf node
            int nonleaf_length = length_of_nonleaf(nonleaf);
            int idx_temp = idx_in_array(nonleaf->keyArray, nonleaf_length - 1, this->lowValInt);
            int index = (idx_temp >= nonleaf_length) ? nonleaf_length : idx_temp;
            this->currentPageNum = nonleaf->pageNoArray[index];
        }

        LeafNodeInt *leaf = (LeafNodeInt *) this->currentPageData;
        int leaf_length = length_of_leaf(leaf);
        int idx_temp;
        // find index of leaf
        if (this->lowOp == GTE)
            idx_temp = idx_in_array(leaf->keyArray, leaf_length, this->lowValInt);
        else
            idx_temp = idx_in_array(leaf->keyArray, leaf_length, this->lowValInt + 1);
        int entry_idx = (idx_temp >= leaf_length) ? leaf_length : idx_temp;
        // check if entry_idx is valid or not
        if (entry_idx == -1) {
            this->bufMgr->unPinPage(this->file, this->currentPageNum, false);
            this->currentPageNum = leaf->rightSibPageNo;
            this->bufMgr->readPage(this->file, this->currentPageNum, this->currentPageData);
            this->nextEntry = 0;
        } else {
            this->nextEntry = entry_idx;
        }

}

// -----------------------------------------------------------------------------
// BTreeIndex::scanNext
// -----------------------------------------------------------------------------

const void BTreeIndex::scanNext(RecordId &outRid) {
    if (!this->scanExecuting) throw ScanNotInitializedException();
    if (this->currentPageData == NULL) throw IndexScanCompletedException();

    LeafNodeInt *leaf = (LeafNodeInt *) this->currentPageData;
    // update out rid
    outRid = leaf->ridArray[this->nextEntry];
    if (outRid.page_number == 0 and outRid.slot_number == 0) throw IndexScanCompletedException();
    int key = leaf->keyArray[nextEntry];

    // check if key is in range
    if ((key >= this->highValInt and this->highOp == LT) or
        (key > this->highValInt and this->highOp == LTE) or
        (key <= this->lowValInt and this->lowOp == GT) or
        (key < this->lowValInt and this->lowOp == GTE)) throw IndexScanCompletedException();

    this->nextEntry++;
    // check if entry is in range
    if (this->nextEntry >= this->leafOccupancy or leaf->ridArray[this->nextEntry].page_number == 0) {
        this->bufMgr->unPinPage(this->file, this->currentPageNum, false);
        // update current page number
        this->currentPageNum = leaf->rightSibPageNo;
        this->bufMgr->readPage(this->file, this->currentPageNum, this->currentPageData);
        // a new page is read, so entry number is set back to 0
        nextEntry = 0;
    }
}

// -----------------------------------------------------------------------------
// BTreeIndex::endScan
// -----------------------------------------------------------------------------

const void BTreeIndex::endScan() {
    if (!this->scanExecuting) throw ScanNotInitializedException();
    this->scanExecuting = false;
    this->bufMgr->unPinPage(this->file, this->currentPageNum, false);
    this->nextEntry = -1;
}

}