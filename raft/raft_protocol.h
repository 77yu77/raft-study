#ifndef raft_protocol_h
#define raft_protocol_h

#include "rpc.h"
#include "raft_state_machine.h"

enum raft_rpc_opcodes {
    op_request_vote = 0x1212,
    op_append_entries = 0x3434,
    op_install_snapshot = 0x5656
};

enum raft_rpc_status {
   OK,
   RETRY,
   RPCERR,
   NOENT,
   IOERR
};

class request_vote_args {
public:
    // Your code here
   int term; //candidate’s term
   int candidateId; //candidate requesting vote
   int lastLogIndex; //index of candidate’s last log entry
   int lastLogTerm;//term of candidate’s last log entry

};

marshall& operator<<(marshall &m, const request_vote_args& args);
unmarshall& operator>>(unmarshall &u, request_vote_args& args);


class request_vote_reply {
public:
    // Your code here
    int term;//currentTerm, for candidate to update itself
    bool voteGranted;//true means candidate received vote
};

marshall& operator<<(marshall &m, const request_vote_reply& reply);
unmarshall& operator>>(unmarshall &u, request_vote_reply& reply);

template<typename command>
class log_entry {
public:
    // Your code here
    log_entry(){
          };
    int size();
    int lastIndex();    
    int save_log(command com,int term);
    log_entry<command> sub_log(int headIndex);
    void merge(int pos,log_entry<command> logs);    
    std::vector<command> entries;
    std::vector<int> terms;     
};
template<typename command>
log_entry<command> log_entry<command>::sub_log(int headIndex){
    log_entry<command> new_logs;
    for(int i=headIndex;i<terms.size();i++){
     new_logs.entries.push_back(entries[i]);
     new_logs.terms.push_back(terms[i]);
    }
    return new_logs;
}
template<typename command>
int log_entry<command>::save_log(command com,int term){
   entries.push_back(com);
   terms.push_back(term);
   return terms.size()-1;
}

template<typename command>
int log_entry<command>::size(){
   if(entries.empty())
     return 0;
   return terms.size(); 
}

template<typename command>
int log_entry<command>::lastIndex(){
    if(entries.empty())
     return 0;
    return terms.size()-1;
}


template<typename command>
void log_entry<command>::merge(int pos,log_entry<command> logs){

   for(int i=terms.size()-1;i>pos;i--){
       entries.pop_back();
   }
   for(int i=terms.size()-1;i>pos;i--){
       terms.pop_back();
   }  
   for(int i=0;i<logs.entries.size();i++){
       entries.push_back(logs.entries[i]);
       terms.push_back(logs.terms[i]);
   } 
}

template<typename command>
marshall& operator<<(marshall &m, const log_entry<command>& entry) {
    // Your code here
    m << entry.entries;
    m << entry.terms;
    return m;
}

template<typename command>
unmarshall& operator>>(unmarshall &u, log_entry<command>& entry) {
    // Your code here
    u >> entry.entries;
    u >> entry.terms; 
    return u;
}

template<typename command>
class append_entries_args {
public:
    int term;
    int leaderId;
    int prevLogIndex;
    int prevLogTerm;
    int leaderCommit;
    log_entry<command> entries;
    // Your code here
};

template<typename command>
marshall& operator<<(marshall &m, const append_entries_args<command>& args) {
    // Your code here
    m << args.term;
    m << args.leaderId;
    m << args.prevLogIndex;
    m << args.prevLogTerm;
    m << args.leaderCommit;
    m << args.entries;
    return m;
}

template<typename command>
unmarshall& operator>>(unmarshall &u, append_entries_args<command>& args) {
    // Your code here
    u >> args.term;
    u >> args.leaderId;
    u >> args.prevLogIndex;
    u >> args.prevLogTerm;
    u >> args.leaderCommit;
    u >> args.entries;
    return u;
}

class append_entries_reply {
public:
    int term;
    bool success;
    // Your code here
};

marshall& operator<<(marshall &m, const append_entries_reply& reply);
unmarshall& operator>>(unmarshall &m, append_entries_reply& reply);


class install_snapshot_args {
public:
    // Your code here
};

marshall& operator<<(marshall &m, const install_snapshot_args& args);
unmarshall& operator>>(unmarshall &m, install_snapshot_args& args);


class install_snapshot_reply {
public:
    // Your code here
};

marshall& operator<<(marshall &m, const install_snapshot_reply& reply);
unmarshall& operator>>(unmarshall &m, install_snapshot_reply& reply);


#endif // raft_protocol_h