#include "inode_manager.h"
#include "time.h"
#include <iostream>
// disk layer -----------------------------------------

disk::disk() { bzero(blocks, sizeof(blocks)); }

void disk::read_block(blockid_t id, char *buf) {
  memcpy(buf, blocks[id], BLOCK_SIZE);
}

void disk::write_block(blockid_t id, const char *buf) {
  memcpy(blocks[id], buf, BLOCK_SIZE);
}

// block layer -----------------------------------------

// Allocate a free disk block.
blockid_t block_manager::alloc_block() {
  /*
   * your code goes here.
   * note: you should mark the corresponding bit in block bitmap when alloc.
   * you need to think about which block you can start to be allocated.
   */
  int bitblocknum = BBLOCK(0) + BLOCK_NUM / BPB;
  int blocknum = 0;
  char blockBPB[BLOCK_SIZE];
  for (blocknum = 0; blocknum < BLOCK_NUM; blocknum += BPB) {
    read_block(BBLOCK(blocknum), blockBPB);
    for (u_int offset = 0; offset < BPB; offset++) {
      //防止读到IBLOCK或之前的块的bitmap
      if (offset + blocknum < IBLOCK(INODE_NUM, BLOCK_NUM))
        continue;
      char bit = (char)(1 << (offset % 8));
      if ((blockBPB[offset / 8] & bit) == 0) {
        blockBPB[offset / 8] |= bit;
        write_block(BBLOCK(blocknum), blockBPB);
        return blocknum + offset;
      }
    }
  }
  return 0;
}

void block_manager::free_block(uint32_t id) {
  /*
   * your code goes here.
   * note: you should unmark the corresponding bit in the block bitmap when
   * free.
   */
  if (id <= BBLOCK(BLOCK_NUM))
    return;
  // printf("free_block%d\n",id);
  char t[BLOCK_SIZE];
  read_block(BBLOCK(id), t);
  int pos1 = id % BPB;
  int pos2 = pos1 / 8;
  int pos3 = pos1 % 8;
  t[pos2] = t[pos2] & (~(1 << pos3));
  write_block(BBLOCK(id), t);
  return;
}

// The layout of disk should be like this:
// |<-sb->|<-free block bitmap->|<-inode table->|<-data->|
block_manager::block_manager() {
  d = new disk();

  // format the disk
  sb.size = BLOCK_SIZE * BLOCK_NUM;
  sb.nblocks = BLOCK_NUM;
  sb.ninodes = INODE_NUM;
}

void block_manager::read_block(uint32_t id, char *buf) {
  d->read_block(id, buf);
}

void block_manager::write_block(uint32_t id, const char *buf) {
  d->write_block(id, buf);
}

// inode layer -----------------------------------------

inode_manager::inode_manager() {
  bm = new block_manager();
  uint32_t root_dir = alloc_inode(extent_protocol::T_DIR);
  if (root_dir != 1) {
    printf("\tim: error! alloc first inode %d, should be 1\n", root_dir);
    exit(0);
  }
}

/* Create a new file.
 * Return its inum. */
uint32_t inode_manager::alloc_inode(uint32_t type) {
  /*
   * your code goes here.
   * note: the normal inode block should begin from the 2nd inode block.
   * the 1st is used for root_dir, see inode_manager::inode_manager().
   * T_FILE
   */
  char buf[BLOCK_SIZE];

  uint inum = 0, off = 0;
  for (inum = 1; inum < bm->sb.ninodes; ++inum) {
    off = (inum - 1) % IPB;
    if (!off)
      bm->read_block(IBLOCK(inum, bm->sb.nblocks), buf);
    inode_t *inode = (inode_t *)buf + off;
    if (!inode->type) {
      inode->type = (short)type;
      inode->ctime = (uint)time(NULL);
      bm->write_block(IBLOCK(inum, bm->sb.nblocks), buf);
      return inum;
    }
  }
  return 1;
}

void inode_manager::free_inode(uint32_t inum) {
  /*
   * your code goes here.
   * note: you need to check if the inode is already a freed one;
   * if not, clear it, and remember to write back to disk.
   */
  inode *t = get_inode(inum);
  if (t->type == 0)
    printf("the inode is already freed");
  else {
    t->type = 0;
    t->ctime = 0;
    t->atime = 0;
    t->mtime = 0;
    t->size = 0;
    put_inode(inum, t);
  }
  delete t;
  return;
}

/* Return an inode structure by inum, NULL otherwise.
 * Caller should release the memory. */
struct inode *inode_manager::get_inode(uint32_t inum) {
  struct inode *ino, *ino_disk;
  char buf[BLOCK_SIZE];

  printf("\tim: get_inode %d\n", inum);

  if (inum < 0 || inum >= INODE_NUM) {
    printf("\tim: inum out of range\n");
    return NULL;
  }

  bm->read_block(IBLOCK(inum, bm->sb.nblocks), buf);
  // printf("%s:%d\n", __FILE__, __LINE__);

  ino_disk = (struct inode *)buf + inum % IPB;
  if (ino_disk->type == 0) {
    printf("\tim: inode not exist\n");
    return NULL;
  }

  ino = (struct inode *)malloc(sizeof(struct inode));
  *ino = *ino_disk;

  return ino;
}

void inode_manager::put_inode(uint32_t inum, struct inode *ino) {
  char buf[BLOCK_SIZE];
  struct inode *ino_disk;

  printf("\tim: put_inode %d\n", inum);
  if (ino == NULL)
    return;

  bm->read_block(IBLOCK(inum, bm->sb.nblocks), buf);
  ino_disk = (struct inode *)buf + inum % IPB;
  *ino_disk = *ino;
  bm->write_block(IBLOCK(inum, bm->sb.nblocks), buf);
}

#define MIN(a, b) ((a) < (b) ? (a) : (b))

/* Get all the data of a file by inum.
 * Return alloced data, should be freed by caller. */
void inode_manager::read_file(uint32_t inum, char **buf_out, int *size) {
  /*
   * your code goes here.
   * note: read blocks related to inode number inum,
   * and copy them to buf_out
   */

  printf("read_file%d\n", inum);
  inode *t = get_inode(inum);
  if (t == NULL)
    return;

  char *tobuf_out = new char[t->size];
  // 接受最后一块可能没填满的block
  char final_buf[BLOCK_SIZE];
  u_int now_time = (u_int)time(NULL);
  t->atime = now_time;
  t->ctime = now_time;
  *size = t->size;
  // pos为inode当前block位置，rec_size为当前长度
  uint32_t pos = 0, rec_size = 0;
  for (pos = 0; rec_size < t->size && pos < NDIRECT; pos++) {
    if (rec_size + BLOCK_SIZE < t->size) {
      bm->read_block(t->blocks[pos], tobuf_out + rec_size);
      rec_size += BLOCK_SIZE;
    } else {
      bm->read_block(t->blocks[pos], final_buf);
      //  printf("blocknum%d\n",t->blocks[pos]);
      memcpy(tobuf_out + rec_size, final_buf, t->size - rec_size);
      rec_size = t->size;
      //  printf("no much than 100 size:%d\n",t->size);
    }
  }

  //读取二级页表
  if (rec_size < t->size) {
    //  printf("much than 100\n");
    blockid_t d[NINDIRECT];
    bm->read_block(t->blocks[NDIRECT], (char *)d);
    for (pos = 0; rec_size < t->size && pos < NINDIRECT; pos++) {
      if (rec_size + BLOCK_SIZE < t->size) {
        bm->read_block(d[pos], tobuf_out + rec_size);
        rec_size += BLOCK_SIZE;
      } else {
        bm->read_block(d[pos], final_buf);
        memcpy(tobuf_out + rec_size, final_buf, t->size - rec_size);
        rec_size = t->size;
      }
    }
  }
  *buf_out = tobuf_out;
  put_inode(inum, t);
  delete t;
  return;
}

/* alloc/free blocks if needed */
void inode_manager::write_file(uint32_t inum, const char *buf, int size) {
  /*
   * your code goes here.
   * note: write buf to blocks of inode inum.
   * you need to consider the situation when the size of buf
   * is larger or smaller than the size of original inode
   */

  printf("write_file%d\n", inum);
  inode *t = get_inode(inum);
  std::string k = std::string(buf);
  int beforeblocknum = (t->size + BLOCK_SIZE - 1) / BLOCK_SIZE;
  int afterblocknum = (size + BLOCK_SIZE - 1) / BLOCK_SIZE;
  // printf("before %d\n",beforeblocknum);
  // printf("after %d\n",afterblocknum);
  //将原本的空间清空
  if (beforeblocknum > NDIRECT) {
    for (int i = 0; i < NDIRECT; i++) {
      bm->free_block(t->blocks[i]);
    }
    blockid_t d[NINDIRECT];
    bm->read_block(t->blocks[NDIRECT], (char *)d);
    int dnum = beforeblocknum - NDIRECT;
    for (int i = 0; i < dnum; i++) {
      bm->free_block(d[i]);
    }
    bm->free_block(t->blocks[NDIRECT]);
  } else {
    for (int i = 0; i < beforeblocknum; i++) {
      bm->free_block(t->blocks[i]);
    }
  }
  // printf("free success\n");
  //申请新的空间
  if (afterblocknum > NDIRECT) {
    for (int i = 0; i <= NDIRECT; i++) {
      t->blocks[i] = bm->alloc_block();
      //  printf("%d\n",t->blocks[i]);
    }
    blockid_t d[NINDIRECT];
    for (int i = 0; i < afterblocknum - NDIRECT; i++) {
      d[i] = bm->alloc_block();
      // printf("%d\n",d[i]);
    }
    bm->write_block(t->blocks[NDIRECT], (char *)d);
  } else {
    for (int i = 0; i < afterblocknum; i++) {
      t->blocks[i] = bm->alloc_block();
      // printf("%d\n",t->blocks[i]);
    }
  }
  printf("alloc success\n");
  //写进block
  int rec_size = 0;
  int pos = 0;
  char final_block[BLOCK_SIZE];
  for (pos = 0; rec_size < size && pos < NDIRECT; pos++) {
    if (rec_size + BLOCK_SIZE < size) {
      bm->write_block(t->blocks[pos], buf + rec_size);
      rec_size += BLOCK_SIZE;
    } else {
      memcpy(final_block, buf + rec_size, size - rec_size);
      std::string s = final_block;
      std::cout << s << std::endl;
      std::cout << "blocknum:" << t->blocks[pos] << std::endl;
      bm->write_block(t->blocks[pos], final_block);
      rec_size = size;
    }
  }

  //写进二级页表
  if (rec_size < size) {
    // printf("to much for write\n");
    blockid_t d[NINDIRECT];
    bm->read_block(t->blocks[NDIRECT], (char *)d);
    for (pos = 0; rec_size < size && pos < NINDIRECT; pos++) {
      if (rec_size + BLOCK_SIZE < size) {
        // printf("write %d\n",d[pos]);
        bm->write_block(d[pos], buf + rec_size);
        rec_size += BLOCK_SIZE;
      } else {
        memcpy(final_block, buf + rec_size, size - rec_size);
        // printf("?\n");
        bm->write_block(d[pos], final_block);
        rec_size = size;
      }
    }
  }
  u_int now_time = (u_int)time(NULL);
  t->size = (u_int)size;
  t->ctime = now_time;
  t->mtime = now_time;
  put_inode(inum, t);
  // printf("write size:%d\n",t->size);
  delete t;
  return;
}

void inode_manager::getattr(uint32_t inum, extent_protocol::attr &a) {
  /*
   * your code goes here.
   * note: get the attributes of inode inum.
   * you can refer to "struct attr" in extent_protocol.h
   */
  printf("getattr %d\n", inum);
  // no inode
  inode *t = get_inode(inum);
  if (t == NULL)
    return;
  a.atime = t->atime;
  a.ctime = t->ctime;
  a.mtime = t->mtime;
  a.size = t->size;
  a.type = t->type;
  delete t;
  return;
}

void inode_manager::remove_file(uint32_t inum) {
  /*
   * your code goes here
   * note: you need to consider about both the data block and inode of the file
   */
  printf("remove_file\n");
  inode *t = get_inode(inum);
  if (t == NULL) {
    // printf("remove_file:no inode\n");
    return;
  }
  int blocknum = (t->size + BLOCK_SIZE - 1) / BLOCK_SIZE;
  // printf("remove_block%d\n",blocknum);

  if (blocknum <= NDIRECT) {
    for (int i = 0; i < blocknum; i++) {
      bm->free_block(t->blocks[i]);
      // printf("free num:%d\n",t->blocks[i]);
    }
  } else {
    for (int i = 0; i < NDIRECT; i++) {
      bm->free_block(t->blocks[i]);
    }
    blockid_t d[NINDIRECT];
    bm->read_block(t->blocks[NDIRECT], (char *)d);
    for (int i = 0; i < blocknum - NDIRECT; i++) {
      bm->free_block(d[i]);
      // printf("free num:%d %d\n",d[i-100],i);
    }
    //将存放二级页表的inode释放
    bm->free_block(t->blocks[NDIRECT]);
  }
  free_inode(inum);
  delete t;
  return;
}
