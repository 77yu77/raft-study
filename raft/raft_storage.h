#ifndef raft_storage_h
#define raft_storage_h

#include "raft_protocol.h"
#include<iostream>
#include <fcntl.h>
#include <fstream>
#include <mutex>
#include <stdio.h>
    struct Metadata{
       int id;
       int current_term;
       int votedFor;
      //  int commit_index;
      //  int last_applied;
    };
template<typename command>
class raft_storage {
public:   

    raft_storage(const std::string& file_dir);
    ~raft_storage();
    bool save_votedFor(int votedFor);
    bool save_currentTerm(int current_term);
    bool save_commit_index(int commit_index);
    bool save_last_applied(int last_applied);
    bool defineId(int id);
    bool havePresisted();
    bool savelogs(command cmd,int term);
    bool overwritelogs(log_entry<command> logs);
    log_entry<command> get_logs();
    Metadata getMetadata();
    std::string dir;
    
private:
    std::string logstring;
    std::string metedatastring;
    std::string termstring;
    std::string logsizestring;
    
    Metadata metadata; 
    bool havePresist;   
    std::mutex mtx;
};

template<typename command>
raft_storage<command>::raft_storage(const std::string& dir){
    // Your code here 
    this->dir=dir;
    logstring=dir+"/log";
    metedatastring=dir+"/metedata";
    termstring=dir+"/term";
    logsizestring=dir+"/logsize";
    havePresist=false;
    metadata.current_term=0;
    metadata.id=-1;
    metadata.votedFor=-1;
    // metadata.commit_index=0;
    // metadata.last_applied=0;
}

template<typename command>
raft_storage<command>::~raft_storage() {
   // Your code here
}
template<typename command>
bool raft_storage<command>::defineId(int id){
   mtx.lock();
   logstring+=std::to_string(id)+".txt";
   metedatastring+=std::to_string(id)+".txt";
   termstring+=std::to_string(id)+".txt";
   logsizestring+=std::to_string(id)+".txt";
   FILE* fp;
    if(fp=fopen(metedatastring.c_str(),"r")) {
        fread((void *)&metadata,sizeof(Metadata),1,fp);
        fclose(fp);
        havePresist=true;
        mtx.unlock();
        return true;
    }
   //metadata is empty 
   if((fp=fopen(metedatastring.c_str(),"w"))==NULL){
     mtx.unlock();
     return false; 
   };
    fwrite((void *)&metadata,sizeof(Metadata),1,fp);
    fclose(fp);

   if((fp=fopen(logstring.c_str(),"w"))==NULL){
     mtx.unlock();     
     return false; 
   };
   fclose(fp); 
   if((fp=fopen(termstring.c_str(),"w"))==NULL){
     mtx.unlock();
     return false; 
   };
   fclose(fp);
   if((fp=fopen(logsizestring.c_str(),"w"))==NULL){
     
     mtx.unlock();
     return false; 
   };
   fclose(fp);  
   mtx.unlock();   
   return false; 
}

template<typename command>
bool raft_storage<command>::save_votedFor(int votedFor){
   mtx.lock();
   FILE* fp;    
   if((fp=fopen(metedatastring.c_str(),"w"))==NULL){
     mtx.unlock();
     return false; 
   };
   metadata.votedFor=votedFor;
   fwrite((void *)&metadata,sizeof(Metadata),1,fp);
   fclose(fp);
   mtx.unlock();
   return true;
}

template<typename command>
bool raft_storage<command>::save_currentTerm(int current_term){
   mtx.lock();
   FILE* fp;
   if((fp=fopen(metedatastring.c_str(),"w"))==NULL){
     mtx.unlock();
     return false; 
   };
   metadata.current_term=current_term;
   fwrite((void *)&metadata,sizeof(Metadata),1,fp);
   fclose(fp);
   mtx.unlock();
   return true;
}
template<typename command>
log_entry<command> raft_storage<command>::get_logs(){ 
  
  mtx.lock();
  log_entry<command> logs;
  command com;
  // logs.entries.push_back(com);
  // logs.terms.push_back(0);

  std::vector<int> logsizearray;
  // FILE *fp;
  std::ifstream logsizefs;
  logsizefs.open(logsizestring);
  int logsize;
  ;
  while(logsizefs.peek()!=EOF){
    logsizefs.read((char *)&logsize,sizeof(int));
    logsizearray.push_back(logsize);  
  } 
  logsizefs.close(); 
  
  std::vector<int> terms;
  int term;
  std::ifstream termsfs;
  termsfs.open(termstring);
  while(termsfs.peek()!=EOF){
   termsfs.read((char *)&term,sizeof(int));
   logs.terms.push_back(term);   
  }
  termsfs.close();
  
  std::ifstream logfs;
  logfs.open(logstring);  
  char *data;
   
  for(int i=0;i<logsizearray.size();i++){
    data=new char[logsizearray[i]];
    logfs.read(data,logsizearray[i]);  
    com.deserialize(data,logsizearray[i]);
    logs.entries.push_back(com);
    delete [] data;
  }  
  logfs.close();
  mtx.unlock();
  return logs;

};

template<typename command>
bool raft_storage<command>::havePresisted(){
  return havePresist;
}
template<typename command>
Metadata raft_storage<command>::getMetadata(){
  return metadata;
}
template<typename command>
bool raft_storage<command>::savelogs(command cmd,int term){
   mtx.unlock();
   std::ofstream logsfs;
   std::ofstream logsizefs;
   std::ofstream termfs;
   logsfs.open(logstring,std::ios::app);
   logsizefs.open(logsizestring,std::ios::app);
   termfs.open(termstring,std::ios::app);
   char *t=new char[cmd.size()];
   cmd.serialize(t,cmd.size());
   logsfs.write((char *)t,cmd.size());
   int size=cmd.size();
   logsizefs.write((char *)&size,sizeof(int));
   termfs.write((char *)&term,sizeof(int));
   logsfs.close();
   logsizefs.close();
   termfs.close();
   mtx.unlock();
   return true; 
}
template<typename command>
bool raft_storage<command>::overwritelogs(log_entry<command> logs){
   mtx.lock();
   std::ofstream logsfs;
   std::ofstream logsizefs;
   std::ofstream termfs;  
   logsfs.open(logstring,std::ios::out|std::ios::trunc);
   logsizefs.open(logsizestring,std::ios::out|std::ios::trunc);
   termfs.open(termstring,std::ios::out|std::ios::trunc);
  //  FILE *logsfp;
  //  FILE *logsizefp;
  //  FILE *termsfp;
  //  if((logsfp=fopen(logstring.c_str(),"w"))==NULL){
  //    return false; 
  //  };
  //  if((logsizefp=fopen(logsizestring.c_str(),"w"))==NULL){
  //    return false; 
  //  };
  //  if((termsfp=fopen(termstring.c_str(),"w"))==NULL){
  //    return false; 
  //  };
   for(int i=1;i<logs.entries.size();i++){
    char *t=new char[logs.entries[i].size()];
    logs.entries[i].serialize(t,logs.entries[i].size());
    logsfs.write((char *)t,logs.entries[i].size());
    // fwrite((void *)t,logs.entries[i].size(),1,logsfp);
    int size=logs.entries[i].size();
    // fwrite((void *)&size,sizeof(int),1,logsizefp);
    // int term=logs.terms[i];
    // fwrite((void *)&term,sizeof(int),1,termsfp);
    logsizefs.write((char *)&size,sizeof(int));
    termfs.write((char *)&(logs.terms[i]),sizeof(int)); 
    delete [] t;      
   } 
   mtx.unlock(); 
   return true;  
};
#endif // raft_storage_h