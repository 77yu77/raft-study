// RPC stubs for clients to talk to extent_server

#include "extent_client.h"
#include <sstream>
#include <iostream>
#include <stdio.h>
#include <unistd.h>
#include <time.h>

extent_client::extent_client(std::string dst)
{
  sockaddr_in dstsock;
  make_sockaddr(dst.c_str(), &dstsock);
  cl = new rpcc(dstsock);
  if (cl->bind() != 0) {
    printf("extent_client: bind failed\n");
  }
}

extent_protocol::status
extent_client::create(uint32_t type, extent_protocol::extentid_t &id)
{ 
  printf("create\n");
  extent_protocol::status ret=cl->call(extent_protocol::create,type,id);
  // Your lab2 part1 code goes here
  return ret;
}

extent_protocol::status
extent_client::get(extent_protocol::extentid_t eid, std::string &buf)
{
  printf("get\n");
  extent_protocol::status ret = cl->call(extent_protocol::get,eid,buf);
  // Your lab2 part1 code goes here
  return ret;
}

extent_protocol::status
extent_client::getattr(extent_protocol::extentid_t eid, 
		       extent_protocol::attr &attr)
{
  printf("getattr\n");
  extent_protocol::status ret = cl->call(extent_protocol::getattr,eid,attr);
  // Your lab2 part1 code goes here
  return ret;
}

extent_protocol::status
extent_client::put(extent_protocol::extentid_t eid, std::string buf)
{
  printf("put\n");
  int r;
  extent_protocol::status ret = cl->call(extent_protocol::put,eid,buf,r);
  // Your lab2 part1 code goes here
  return ret;
}

extent_protocol::status
extent_client::remove(extent_protocol::extentid_t eid)
{ 
  printf("remove\n");
  int r;
  extent_protocol::status ret = cl->call(extent_protocol::remove,eid,r);
  // Your lab2 part1 code goes here
  return ret;
}