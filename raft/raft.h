#ifndef raft_h
#define raft_h

#include <algorithm>
#include <atomic>
#include <chrono>
#include <ctime>
#include <mutex>
#include <stdarg.h>
#include <thread>

#include "raft_protocol.h"
#include "raft_state_machine.h"
#include "raft_storage.h"
#include "rpc.h"

template <typename state_machine, typename command> class raft {

  static_assert(std::is_base_of<raft_state_machine, state_machine>(),
                "state_machine must inherit from raft_state_machine");
  static_assert(std::is_base_of<raft_command, command>(),
                "command must inherit from raft_command");

  friend class thread_pool;

#define RAFT_LOG(fmt, args...)                                                 \
  do {                                                                         \
    auto now = std::chrono::duration_cast<std::chrono::milliseconds>(          \
                   std::chrono::system_clock::now().time_since_epoch())        \
                   .count();                                                   \
    printf("[%ld][%s:%d][node %d term %d] " fmt "\n", now, __FILE__, __LINE__, \
           my_id, current_term, ##args);                                       \
  } while (0);

public:
  raft(rpcs *rpc_server, std::vector<rpcc *> rpc_clients, int idx,
       raft_storage<command> *storage, state_machine *state);
  ~raft();

  // start the raft node.
  // Please make sure all of the rpc request handlers have been registered
  // before this method.
  void start();

  // stop the raft node.
  // Please make sure all of the background threads are joined in this method.
  // Notice: you should check whether is server should be stopped by calling
  // is_stopped().
  //         Once it returns true, you should break all of your long-running
  //         loops in the background threads.
  void stop();

  // send a new command to the raft nodes.
  // This method returns true if this raft node is the leader that successfully
  // appends the log. If this node is not the leader, returns false.
  bool new_command(command cmd, int &term, int &index);

  // returns whether this node is the leader, you should also set the current
  // term;
  bool is_leader(int &term);

  // save a snapshot of the state machine and compact the log.
  bool save_snapshot();
  int64_t getNowTime() {
    return std::chrono::duration_cast<std::chrono::milliseconds>(
               std::chrono::system_clock::now().time_since_epoch())
        .count();
  }

private:
  std::mutex mtx; // A big lock to protect the whole data structure
  ThrPool *thread_pool;
  raft_storage<command> *storage; // To persist the raft log log信息
  log_entry<command> logs;
  state_machine
      *state; // The state machine that applies the raft log, e.g. a kv store

  rpcs *rpc_server; // RPC server to recieve and handle the RPC requests
  std::vector<rpcc *>
      rpc_clients; // RPC clients of all raft nodes including this node
  int my_id;       // The index of this node in rpc_clients, start from 0

  std::atomic_bool stopped;

  enum raft_role { follower, candidate, leader };
  raft_role role; //服务器身份
  int votedFor;
  int current_term; //服务器看到的最新term
  int commit_index; //已知最高的日志条目的索引
  int last_applied; //应用于状态机的最高日志条目的索引
  int votenum;
  int64_t last_received_RPC_time;
  std::thread *background_election;
  std::thread *background_ping;
  std::thread *background_commit;
  std::thread *background_apply;
  std::vector<int>
      nextIndex; //对于每个服务器，下一个日志条目的索引 发送给该服务器的索引
  std::vector<int> matchIndex; //每个服务器上被复制的最高日志条目的索引
                               // Your code here:

private:
  // RPC handlers
  int request_vote(request_vote_args arg, request_vote_reply &reply);

  int append_entries(append_entries_args<command> arg,
                     append_entries_reply &reply);

  int install_snapshot(install_snapshot_args arg,
                       install_snapshot_reply &reply);

  // RPC helpers
  void send_request_vote(int target, request_vote_args arg);
  void handle_request_vote_reply(int target, const request_vote_args &arg,
                                 const request_vote_reply &reply);

  void send_append_entries(int target, append_entries_args<command> arg);
  void handle_append_entries_reply(int target,
                                   const append_entries_args<command> &arg,
                                   const append_entries_reply &reply);

  void send_install_snapshot(int target, install_snapshot_args arg);
  void handle_install_snapshot_reply(int target,
                                     const install_snapshot_args &arg,
                                     const install_snapshot_reply &reply);

private:
  bool is_stopped();
  int num_nodes() { return rpc_clients.size(); }

  // background workers
  void run_background_ping();
  void run_background_election();
  void run_background_commit();
  void run_background_apply();

  // Your code here:
};

template <typename state_machine, typename command>
raft<state_machine, command>::raft(rpcs *server, std::vector<rpcc *> clients,
                                   int idx, raft_storage<command> *storage,
                                   state_machine *state)
    : storage(storage), state(state), rpc_server(server), rpc_clients(clients),
      my_id(idx), stopped(false), role(follower), votedFor(-1), current_term(0),
      commit_index(0), last_applied(0), votenum(0),
      background_election(nullptr), background_ping(nullptr),
      background_commit(nullptr), background_apply(nullptr) {
  thread_pool = new ThrPool(32);
  for (int i = 0; i < rpc_clients.size(); i++) {
    nextIndex.push_back(1);
    matchIndex.push_back(0);
  }

  //确认数据id
  storage->defineId(idx);
  command com;
  logs.entries.push_back(com);
  logs.terms.push_back(0);

  if (typeid(this->state) == typeid(kv_state_machine *)) {
    RAFT_LOG("change type success");
    std::string path =
        storage->dir + "kv_store" + std::to_string(my_id) + ".txt";
    dynamic_cast<kv_state_machine *>(this->state)->getpath(path);
  }
  //恢复数据
  if (storage->havePresisted()) {
    log_entry<command> logsentry = storage->get_logs();
    votedFor = storage->getMetadata().votedFor;
    current_term = storage->getMetadata().current_term;
    for (int i = 0; i < logsentry.entries.size(); i++) {
      logs.save_log(logsentry.entries[i], logsentry.terms[i]);
      // RAFT_LOG("node %d recover log %d term %d",my_id,i,logsentry.terms[i]);
    }
  }
  // Register the rpcs.
  rpc_server->reg(raft_rpc_opcodes::op_request_vote, this, &raft::request_vote);
  rpc_server->reg(raft_rpc_opcodes::op_append_entries, this,
                  &raft::append_entries);
  rpc_server->reg(raft_rpc_opcodes::op_install_snapshot, this,
                  &raft::install_snapshot);

  // Your code here:
  // Do the initialization
}

template <typename state_machine, typename command>
raft<state_machine, command>::~raft() {
  if (background_ping) {
    delete background_ping;
  }
  if (background_election) {
    delete background_election;
  }
  if (background_commit) {
    delete background_commit;
  }
  if (background_apply) {
    delete background_apply;
  }
  delete thread_pool;
}

/******************************************************************

                        Public Interfaces

*******************************************************************/

template <typename state_machine, typename command>
void raft<state_machine, command>::stop() {
  stopped.store(true);
  background_ping->join();
  background_election->join();
  background_commit->join();
  background_apply->join();
  thread_pool->destroy();
}

template <typename state_machine, typename command>
bool raft<state_machine, command>::is_stopped() {
  return stopped.load();
}

template <typename state_machine, typename command>
bool raft<state_machine, command>::is_leader(int &term) {
  term = current_term;
  return role == leader;
}

template <typename state_machine, typename command>
void raft<state_machine, command>::start() {
  // Your code here:
  last_received_RPC_time = getNowTime();
  RAFT_LOG("start");
  this->background_election =
      new std::thread(&raft::run_background_election, this);
  this->background_ping = new std::thread(&raft::run_background_ping, this);
  this->background_commit = new std::thread(&raft::run_background_commit, this);
  this->background_apply = new std::thread(&raft::run_background_apply, this);
}

template <typename state_machine, typename command>
bool raft<state_machine, command>::new_command(command cmd, int &term,
                                               int &index) {
  // Your code here:
  // not the leader
  mtx.lock();
  if (role != leader) {
    mtx.unlock();
    return false;
  }
  term = current_term;
  // RAFT_LOG("%d get new command",my_id);
  // storage->savelogs(cmd,term);
  log_entry<command> tmp_entry = logs;
  tmp_entry.save_log(cmd, term);
  storage->overwritelogs(tmp_entry);
  index = logs.save_log(cmd, term);
  mtx.unlock();
  return true;
}

template <typename state_machine, typename command>
bool raft<state_machine, command>::save_snapshot() {
  // Your code here:
  return true;
}

/******************************************************************

                         RPC Related

*******************************************************************/
template <typename state_machine, typename command>
int raft<state_machine, command>::request_vote(request_vote_args args,
                                               request_vote_reply &reply) {
  // Your code here:
  mtx.lock();
  // RAFT_LOG("%d receive request_vote from %d",my_id,args.candidateId);
  last_received_RPC_time = getNowTime();
  if (args.term < current_term) {
    reply.voteGranted = false;
    reply.term = current_term;
    mtx.unlock();
    return 0;
  }
  if (current_term < args.term) {
    storage->save_currentTerm(args.term);
    current_term = args.term;
  }
  // RAFT_LOG("arg.lastLogterm %d arg.lastlogindex %d logs.lastlogterm %d
  // logs.lastlogindex
  // %d",args.lastLogTerm,args.lastLogIndex,logs.terms.back(),logs.lastIndex());
  if ((votedFor == -1) || (args.lastLogTerm > logs.terms.back()) ||
      ((args.lastLogTerm == logs.terms.back()) &&
       (args.lastLogIndex >= logs.lastIndex()))) {
    storage->save_votedFor(args.candidateId);
    votedFor = args.candidateId;
    reply.voteGranted = true;
  } else {
    reply.voteGranted = false;
  }
  reply.term = current_term;
  mtx.unlock();

  return 0;
}

template <typename state_machine, typename command>
void raft<state_machine, command>::handle_request_vote_reply(
    int target, const request_vote_args &arg, const request_vote_reply &reply) {
  // Your code here:
  mtx.lock();
  if (reply.term > current_term) {
    storage->save_currentTerm(reply.term);
    current_term = reply.term;
    role = follower;
    last_received_RPC_time = getNowTime();
    mtx.unlock();
    return;
  }

  if (role == follower) {
    mtx.unlock();
    return;
  }
  last_received_RPC_time = getNowTime();
  if (reply.voteGranted) {
    votenum++;
    RAFT_LOG("%d have role for %d", target, my_id);
    if ((votenum > (rpc_clients.size() / 2)) && (role != leader)) {
      role = leader;
      // initializes all nextIndex values
      int nextlog = logs.size();
      for (int i = 0; i < nextIndex.size(); i++) {
        nextIndex[i] = nextlog;
      }
      RAFT_LOG("%d is leader", my_id);
    }
  }
  mtx.unlock();
  return;
}

template <typename state_machine, typename command>
int raft<state_machine, command>::append_entries(
    append_entries_args<command> arg, append_entries_reply &reply) {
  // Your code here:
  // std::unique_lock<std::mutex> lock(mtx);

  mtx.lock();
  if (arg.term < current_term) {
    reply.success = false;
    reply.term = current_term;
    mtx.unlock();
    return 0;
  }
  storage->save_currentTerm(arg.term);
  current_term = arg.term;
  reply.term = current_term;
  last_received_RPC_time = getNowTime();
  if (arg.entries.entries.empty()) { // ping
    role = follower;
    // RAFT_LOG("leadindex %d",arg.leaderCommit);
    if (arg.leaderCommit < logs.size() - 1)
      commit_index = arg.leaderCommit;
    else
      commit_index = logs.size() - 1;
    // RAFT_LOG("commit_index %d",commit_index);
    mtx.unlock();
    return 0;
  }

  if ((logs.size() <= arg.prevLogIndex) ||
      (logs.terms[arg.prevLogIndex] != arg.prevLogTerm)) {
    reply.success = false;
  } else {
    // RAFT_LOG("leadindex %d",arg.leaderCommit);
    // RAFT_LOG("%d update the log pos :%d",my_id,arg.prevLogIndex);
    log_entry<command> tmp_entry = logs;
    tmp_entry.merge(arg.prevLogIndex, arg.entries);
    //  RAFT_LOG("node %d tmp_entry length %d",my_id,tmp_entry.entries.size());
    storage->overwritelogs(tmp_entry);
    logs.merge(arg.prevLogIndex, arg.entries);

    // RAFT_LOG("%d log size %d",my_id,logs.size());
    reply.success = true;
    if (arg.leaderCommit < logs.size() - 1)
      commit_index = arg.leaderCommit;
    else
      commit_index = logs.size() - 1;
    // RAFT_LOG("commit_index %d",commit_index);
  }
  mtx.unlock();
  return 0;
}

template <typename state_machine, typename command>
void raft<state_machine, command>::handle_append_entries_reply(
    int target, const append_entries_args<command> &arg,
    const append_entries_reply &reply) {
  // Your code here:
  mtx.lock();
  if (reply.term > current_term) {
    // RAFT_LOG("reply.term %d",reply.term);
    RAFT_LOG("leader update term");
    last_received_RPC_time = getNowTime();
    role = follower;

    storage->save_currentTerm(current_term);
    current_term = reply.term;

    mtx.unlock();
    return;
  }
  if (role == follower) {
    mtx.unlock();
    return;
  }
  if (arg.entries.entries.empty()) { // ping
    mtx.unlock();
    return;
  }
  if (reply.success) {
    // RAFT_LOG("%d succeed to accept commit %d",my_id,target);
    int length = (arg.prevLogIndex + arg.entries.terms.size() + 1);
    if (length > nextIndex[target]) {
      nextIndex[target] = length;
      matchIndex[target] = nextIndex[target] - 1;
      int N = commit_index + 1;
      // RAFT_LOG("leadindex %d",arg.leaderCommit);
      while (N <= logs.terms.size()) {
        int votes = 1;
        for (int i = 0; i < matchIndex.size(); i++) {
          if (i == my_id)
            continue;
          if (matchIndex[i] >= N)
            votes++;
        }
        if (votes > (matchIndex.size() / 2)) {
          if (logs.terms[N] == current_term)
            commit_index = N;
        } else {
          break;
        }
        N++;
      }
    }
    // RAFT_LOG("commitindex %d",commit_index);
    mtx.unlock();
    return;
  }
  // term error
  // RAFT_LOG("%d send commit again to %d",my_id,target);
  nextIndex[target]--;
  mtx.unlock();
  return;
}

template <typename state_machine, typename command>
int raft<state_machine, command>::install_snapshot(
    install_snapshot_args args, install_snapshot_reply &reply) {
  // Your code here:
  return 0;
}

template <typename state_machine, typename command>
void raft<state_machine, command>::handle_install_snapshot_reply(
    int target, const install_snapshot_args &arg,
    const install_snapshot_reply &reply) {
  // Your code here:
  return;
}

template <typename state_machine, typename command>
void raft<state_machine, command>::send_request_vote(int target,
                                                     request_vote_args arg) {
  request_vote_reply reply;

  if (rpc_clients[target]->call(raft_rpc_opcodes::op_request_vote, arg,
                                reply) == 0) {
    handle_request_vote_reply(target, arg, reply);
  } else {
    // RPC fails
  }
}

template <typename state_machine, typename command>
void raft<state_machine, command>::send_append_entries(
    int target, append_entries_args<command> arg) {
  append_entries_reply reply;
  if (rpc_clients[target]->call(raft_rpc_opcodes::op_append_entries, arg,
                                reply) == 0) {
    handle_append_entries_reply(target, arg, reply);
  } else {
    // RPC fails
  }
}

template <typename state_machine, typename command>
void raft<state_machine, command>::send_install_snapshot(
    int target, install_snapshot_args arg) {
  install_snapshot_reply reply;
  if (rpc_clients[target]->call(raft_rpc_opcodes::op_install_snapshot, arg,
                                reply) == 0) {
    handle_install_snapshot_reply(target, arg, reply);
  } else {
    // RPC fails
  }
}

/******************************************************************

                        Background Workers

*******************************************************************/

template <typename state_machine, typename command>
void raft<state_machine, command>::run_background_election() {
  // Check the liveness of the leader.
  // Work for followers and candim,dates.

  // Hints: You should record the time you received the last RPC.
  //        And in this function, you can compare the current time with it.
  //        For example:
  //        if (current_time - last_received_RPC_time > timeout)
  //        start_election(); Actually, the timeout should be different between
  //        the follower (e.g. 300-500ms) and the candidate (e.g. 1s).

  while (true) {
    if (is_stopped())
      return;
    // Your code here:
    mtx.lock();
    int64_t timedelta = getNowTime() - last_received_RPC_time;
    // RAFT_LOG("%d  %ld",my_id,timedelta);
    switch (role) {
    case follower:
      if (timedelta > (300 + my_id * 50)) {
        role = candidate;

        storage->save_votedFor(my_id);
        votedFor = my_id;
        votenum = 1;

        storage->save_currentTerm(current_term + 1);
        current_term++;
        RAFT_LOG("%d become condidate ", my_id);
        last_received_RPC_time = getNowTime();
        request_vote_args arg;
        arg.candidateId = my_id;
        arg.lastLogIndex = logs.lastIndex();
        arg.lastLogTerm = logs.terms.back(); // error
        arg.term = current_term;
        for (int i = 0; i < rpc_clients.size(); i++) {
          if (i == my_id)
            continue;
          thread_pool->addObjJob(this, &raft::send_request_vote, i, arg);
          // send_request_vote(i,arg);
        }
      }
      break;
    case candidate:
      if (timedelta > 1000) {
        storage->save_currentTerm(current_term + 1);
        current_term++;
        votenum = 1;
        request_vote_args arg;
        arg.candidateId = my_id;
        arg.lastLogIndex = logs.lastIndex();
        arg.lastLogTerm = logs.terms.back(); // error
        arg.term = current_term;
        for (int i = 0; i < rpc_clients.size(); i++) {
          if (i == my_id)
            continue;
          thread_pool->addObjJob(this, &raft::send_request_vote, i, arg);
          // send_request_vote(i,arg);
        }
        last_received_RPC_time = getNowTime();
      }
      break;
    default:
      break;
    }
    mtx.unlock();
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
  }

  return;
}

template <typename state_machine, typename command>
void raft<state_machine, command>::run_background_commit() {
  // Send logs/snapshots to the follower.
  // Only work for the leader.

  // Hints: You should check the leader's last log index and the follower's next
  // log index.

  while (true) {
    if (is_stopped())
      return;
    // Your code here:
    mtx.lock();
    if (role == leader) {
      append_entries_args<command> arg;
      arg.term = current_term;
      arg.leaderId = my_id;
      arg.leaderCommit = commit_index;
      for (int i = 0; i < rpc_clients.size(); i++) {
        if (i == my_id)
          continue;
        if (logs.size() > nextIndex[i]) {
          arg.prevLogIndex = nextIndex[i] - 1;
          arg.prevLogTerm = logs.terms[nextIndex[i] - 1];
          arg.entries = logs.sub_log(nextIndex[i]);
          //    RAFT_LOG("commit log to %d",i);
          thread_pool->addObjJob(this, &raft::send_append_entries, i, arg);
        }
      }
    }
    mtx.unlock();
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
  }

  return;
}

template <typename state_machine, typename command>
void raft<state_machine, command>::run_background_apply() {
  // Apply committed logs the state machine
  // Work for all the nodes.

  // Hints: You should check the commit index and the apply index.
  //        Update the apply index and apply the log if commit_index >
  //        apply_index

  while (true) {
    if (is_stopped())
      return;
    // Your code here:
    mtx.lock();
    if (commit_index > last_applied) {
      // RAFT_LOG("%d apply commit commit_index %d",my_id,commit_index);
      for (int i = last_applied + 1;
           (i <= commit_index) && (i <= logs.lastIndex()); i++) {
        state->apply_log(logs.entries[i]);
      }
      last_applied =
          commit_index > logs.lastIndex() ? logs.lastIndex() : commit_index;
    }
    mtx.unlock();
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
  }
  return;
}

template <typename state_machine, typename command>
void raft<state_machine, command>::run_background_ping() {
  // Send empty append_entries RPC to the followers.

  // Only work for the leader.

  while (true) {
    if (is_stopped())
      return;
    // Your code here:
    mtx.lock();
    int datatime = getNowTime() - last_received_RPC_time;
    if (role == leader && datatime > 150) {
      append_entries_args<command> arg;
      arg.leaderCommit = commit_index;
      arg.leaderId = my_id;
      arg.term = current_term;
      log_entry<command> entirs;
      arg.entries = entirs;
      for (int i = 0; i < rpc_clients.size(); i++) {
        if (i == my_id)
          continue;
        // RAFT_LOG("%d send ping to %d",my_id,i);
        thread_pool->addObjJob(this, &raft::send_append_entries, i, arg);
      }
      last_received_RPC_time = getNowTime();
    }
    mtx.unlock();

    std::this_thread::sleep_for(
        std::chrono::milliseconds(10)); // Change the timeout here!
  }
  return;
}

/******************************************************************

                        Other functions

*******************************************************************/

#endif // raft_h