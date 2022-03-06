// chfs client.  implements FS operations using extent server
#include "chfs_client.h"
#include "extent_client.h"
#include <fcntl.h>
#include <iostream>
#include <list>
#include <sstream>
#include <stdio.h>
#include <stdlib.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <time.h>
#include <unistd.h>

chfs_client::chfs_client(std::string extent_dst) {
  ec = new extent_client(extent_dst);
  if (ec->put(1, "") != extent_protocol::OK)
    printf("error init root dir\n"); // XYB: init root dir
}

chfs_client::inum chfs_client::n2i(std::string n) {
  std::istringstream ist(n);
  unsigned long long finum;
  ist >> finum;
  return finum;
}

std::string chfs_client::filename(inum inum) {
  std::ostringstream ost;
  ost << inum;
  return ost.str();
}

bool chfs_client::isfile(inum inum) {
  extent_protocol::attr a;
  if (ec->getattr(inum, a) != extent_protocol::OK) {
    printf("error getting attr\n");
    return false;
  }

  if (a.type == extent_protocol::T_FILE) {
    printf("isfile: %lld is a file\n", inum);
    return true;
  }
  printf("isfile: %lld is a dir\n", inum);
  return false;
}
/** Your code here for Lab...
 * You may need to add routines such as
 * readlink, issymlink here to implement symbolic link.
 *
 * */

bool chfs_client::isdir(inum inum) {
  // Oops! is this still correct when you implement symlink?
  extent_protocol::attr a;
  if (ec->getattr(inum, a) != extent_protocol::OK) {
    printf("error getting attr\n");
    return false;
  }

  if (a.type == extent_protocol::T_DIR) {
    printf("isfile: %lld is a dir\n", inum);
    return true;
  }
  return false;
}

int chfs_client::getfile(inum inum, fileinfo &fin) {
  int r = OK;

  printf("getfile %016llx\n", inum);
  extent_protocol::attr a;
  if (ec->getattr(inum, a) != extent_protocol::OK) {
    r = IOERR;
    goto release;
  }

  fin.atime = a.atime;
  fin.mtime = a.mtime;
  fin.ctime = a.ctime;
  fin.size = a.size;
  printf("getfile %016llx -> sz %llu\n", inum, fin.size);

release:
  return r;
}

int chfs_client::getdir(inum inum, dirinfo &din) {
  int r = OK;

  printf("getdir %016llx\n", inum);
  extent_protocol::attr a;
  if (ec->getattr(inum, a) != extent_protocol::OK) {
    r = IOERR;
    goto release;
  }
  din.atime = a.atime;
  din.mtime = a.mtime;
  din.ctime = a.ctime;

release:
  return r;
}

#define EXT_RPC(xx)                                                            \
  do {                                                                         \
    if ((xx) != extent_protocol::OK) {                                         \
      printf("EXT_RPC Error: %s:%d \n", __FILE__, __LINE__);                   \
      r = IOERR;                                                               \
      goto release;                                                            \
    }                                                                          \
  } while (0)

#define FILELINE 64

// Only support set size of attr
int chfs_client::setattr(inum ino, size_t size) {
  int r = OK;
  printf("setattr\n");
  /*
   * your code goes here.
   * note: get the content of inode ino, and modify its content
   * according to the size (<, =, or >) content length.
   */

  std::string data;
  r = ec->get(ino, data);
  if (r != OK) {
    printf("error: setattr");
    return r;
  }
  data.resize(size);
  r = ec->put(ino, data);
  if (r != OK)
    printf("error: setattr");
  return r;
}

int chfs_client::create(inum parent, const char *name, mode_t mode,
                        inum &ino_out) {
  int r = OK;
  printf("create\n");
  /*
   * your code goes here.
   * note: lookup is what you need to check if file exist;
   * after create file or dir, you must remember to modify the parent
   * infomation.
   */
  bool found = false;
  r = lookup(parent, name, found, ino_out);
  if (found) {
    printf("already create");
    return EXIST;
  }
  ec->create(extent_protocol::T_FILE, ino_out);
  printf("create inode %d\n", ino_out);
  std::string data;
  // save as name?inum?......
  std::string file_name = std::string(name) + '?' + filename(ino_out) + '?';
  r = ec->get(parent, data);
  if (r != OK) {
    printf("error: create");
    return r;
  }
  data += file_name;
  printf("data %ld\n", data.length());
  std::cout << data << std::endl;
  int blocknum = (data.length() + BLOCK_SIZE - 1) / BLOCK_SIZE;
  r = ec->put(parent, data);
  if (r != OK)
    printf("error: create");
  printf("create end\n");
  return r;
}

int chfs_client::mkdir(inum parent, const char *name, mode_t mode,
                       inum &ino_out) {
  int r = OK;
  printf("mkdir\n");
  /*
   * your code goes here.
   * note: lookup is what you need to check if directory exist;
   * after create file or dir, you must remember to modify the parent
   * infomation.
   */
  bool found = false;
  r = lookup(parent, name, found, ino_out);
  if (found) {
    printf("already exist");
    return EXIST;
  }
  ec->create(extent_protocol::T_DIR, ino_out);
  std::string file_name = std::string(name) + '?' + filename(ino_out) + '?';
  std::string data;
  r = ec->get(parent, data);
  if (r != OK) {
    printf("error: mkdir");
    return r;
  }
  data += file_name;
  r = ec->put(parent, data);
  return r;
}

int chfs_client::lookup(inum parent, const char *name, bool &found,
                        inum &ino_out) {
  printf("lookup\n");
  /*
   * your code goes here.
   * note: lookup file from parent dir according to name;
   * you should design the format of directory content.
   */
  found = false;
  int r = OK;
  if (!isdir(parent))
    return r;
  std::list<dirent> dirents;
  // get all dirents
  r = readdir(parent, dirents);
  if (r != OK) {
    printf("error: lookup");
    return r;
  }
  // search for all dirents
  for (std::list<dirent>::iterator i = dirents.begin(); i != dirents.end();
       ++i) {
    if (i->name == std::string(name)) {
      found = true;
      ino_out = i->inum;
      return EXIST;
    }
  }
  return r;
}

int chfs_client::readdir(inum dir, std::list<dirent> &list) {
  int r = OK;
  printf("readdir\n");
  /*
   * your code goes here.
   * note: you should parse the dirctory content using your defined format,
   * and push the dirents to the list.
   */
  std::string data;
  r = ec->get(dir, data);
  if (r != OK) {
    printf("error: readdir");
    return r;
  }
  dirent dirent_one;
  uint64_t pos;
  while (!data.empty()) {
    pos = data.find('?');
    // get name before ?
    dirent_one.name = data.substr(0, pos);
    data = data.substr(pos + 1);
    pos = data.find('?');
    // get inum before ?
    dirent_one.inum = n2i(data.substr(0, pos));
    data = data.substr(pos + 1);
    list.push_back(dirent_one);
  }
  return r;
}

int chfs_client::read(inum ino, size_t size, off_t off, std::string &data) {
  int r = OK;
  printf("read\n");
  /*
   * your code goes here.
   * note: read using ec->get().
   */
  r = ec->get(ino, data);
  if (r != OK) {
    printf("error: read");
    return r;
  }
  // off > EOF,get nothing
  if (off >= (int)data.length()) {
    data = "";
    return r;
  }
  data = data.substr((u_int)off, size);
  return r;
}

int chfs_client::write(inum ino, size_t size, off_t off, const char *data,
                       size_t &bytes_written) {
  int r = OK;
  printf("write\n");
  /*
   * your code goes here.
   * note: write using ec->put().
   * when off > length of original file, fill the holes with '\0'.
   */
  std::string final_data;
  std::string now_data = std::string(data, size);
  r = ec->get(ino, final_data);
  if (r != OK) {
    printf("error: write");
    return r;
  }
  // off>length
  u_long length = final_data.length();
  if (off > (off_t)final_data.length()) {
    final_data.resize((u_long)off);
  }
  final_data.replace((u_long)off, size, now_data);
  r = ec->put(ino, final_data);
  if (r != OK)
    printf("error: write");
  return r;
}

int chfs_client::unlink(inum parent, const char *name) {
  int r = OK;

  /*
   * your code goes here.
   * note: you should remove the file using ec->remove,
   * and update the parent directory content.
   */

  printf("unlink\n");
  bool found = false;
  inum remove_id;
  r = lookup(parent, name, found, remove_id);
  if (!found) {
    printf("error: unlink");
    return chfs_client::NOENT;
  }
  if (isdir(remove_id))
    return chfs_client::NOENT;
  // remove inode
  ec->remove(remove_id);
  std::string data;
  r = ec->get(parent, data);
  if (r != OK) {
    printf("error: unlink");
    return r;
  }
  // delete from parent
  std::string remove_data = std::string(name) + '?' + filename(remove_id) + '?';
  data.replace(data.find(remove_data), remove_data.length(), "");
  r = ec->put(parent, data);
  if (r != OK)
    printf("error: unlink");
  return r;
}

int chfs_client::symlink(inum parent, const char *name, const char *link,
                         inum &ino_out) {

  int r = OK;
  printf("symlink\n");
  bool found = false;
  r = lookup(parent, name, found, ino_out);

  if (found) {
    printf("error: symlink");
    return EXIST;
  }
  // add the inode
  ec->create(extent_protocol::T_SYMLINK, ino_out);
  std::string another_link = std::string(link);
  ec->put(ino_out, another_link);
  // add to parent
  std::string data;
  r = ec->get(parent, data);
  if (r != OK) {
    printf("error: symlink");
    return r;
  }
  data += std::string(name) + '?' + filename(ino_out) + '?';
  r = ec->put(parent, data);
  if (r != OK)
    printf("error: symlink");
  return r;
}

int chfs_client::readlink(inum ino_out, std::string &link) {
  int r = OK;
  printf("readlink\n");
  r = ec->get(ino_out, link);
  return r;
}