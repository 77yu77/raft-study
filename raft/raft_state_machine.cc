#include "raft_state_machine.h"

kv_command::kv_command() : kv_command(CMD_NONE, "", "") {}

kv_command::kv_command(command_type tp, const std::string &key,
                       const std::string &value)
    : cmd_tp(tp), key(key), value(value), res(std::make_shared<result>()) {
  res->start = std::chrono::system_clock::now();
  res->key = key;
}

kv_command::kv_command(const kv_command &cmd)
    : cmd_tp(cmd.cmd_tp), key(cmd.key), value(cmd.value), res(cmd.res) {}

kv_command::~kv_command() {}

int kv_command::size() const {
  // Your code here:
  return sizeof(kv_command);
}

void kv_command::serialize(char *buf, int size) const {
  // Your code here:
  return;
}

void kv_command::deserialize(const char *buf, int size) {
  // Your code here:
  return;
}
marshall &operator<<(marshall &m, const kv_command &cmd) {
  // Your code here:
  m << (int)cmd.cmd_tp;
  m << cmd.key;
  m << cmd.value;
  return m;
}

unmarshall &operator>>(unmarshall &u, kv_command &cmd) {
  // Your code here:
  int num;
  u >> num;
  cmd.cmd_tp = (kv_command::command_type)num;
  u >> cmd.key;
  u >> cmd.value;
  return u;
}

kv_state_machine::~kv_state_machine() {}
void kv_state_machine::apply_log(raft_command &cmd) {
  kv_command &kv_cmd = dynamic_cast<kv_command &>(cmd);
  std::unique_lock<std::mutex> lock(kv_cmd.res->mtx);
  // Your code here:
  kv_cmd.res->done = true;
  kv_cmd.res->key = kv_cmd.key;
  if (kv_cmd.cmd_tp == kv_command::command_type::CMD_GET) {
    auto iter = kv_store.find(kv_cmd.key);
    // printf("get key:%s ;is
    // exist:%d,\n",kv_cmd.key.c_str(),(kv_store.find(kv_cmd.key)!=kv_store.end()));
    if (iter == kv_store.end()) {
      kv_cmd.res->value = "";
      kv_cmd.res->succ = false;
    } else {
      kv_cmd.res->value = iter->second;
      kv_cmd.res->succ = true;
    }
  } else if (kv_cmd.cmd_tp == kv_command::command_type::CMD_PUT) {
    auto iter = kv_store.find(kv_cmd.key);
    if (iter == kv_store.end()) {
      kv_cmd.res->succ = true;
      kv_cmd.res->value = kv_cmd.value;
      std::ofstream fs;
      fs.open(savepath, std::ios::app);
      fs << kv_cmd.key << " " << kv_cmd.value << " ";
      fs.close();
      kv_store[kv_cmd.key] = kv_cmd.value;
      // printf("key is no exist for put:%s\n",kv_cmd.key.c_str());
    } else {
      kv_cmd.res->succ = false;
      kv_cmd.res->value = iter->second;
      kv_store[kv_cmd.key] = kv_cmd.value;
      std::ofstream fs;
      fs.open(savepath, std::ios::out);
      for (std::map<std::string, std::string>::iterator it = kv_store.begin();
           it != kv_store.end(); it++) {
        fs << it->first << " " << it->second << " ";
      }
      fs.close();
      // printf("key is  exist for put:%s\n",kv_cmd.key.c_str());
    }
  } else if (kv_cmd.cmd_tp == kv_command::command_type::CMD_DEL) {
    auto iter = kv_store.find(kv_cmd.key);
    // printf("delete key: %s\n",kv_cmd.key.c_str());
    if (iter == kv_store.end()) {
      kv_cmd.res->succ = false;
      kv_cmd.value = "";
    } else {
      kv_cmd.res->succ = true;
      kv_cmd.res->value = iter->second;
      kv_store.erase(iter);
      std::ofstream fs;
      fs.open(savepath, std::ios::out);
      for (std::map<std::string, std::string>::iterator it = kv_store.begin();
           it != kv_store.end(); it++) {
        fs << it->first << " " << it->second << " ";
      }
      fs.close();
    }
  }
  kv_cmd.res->cv.notify_all();
  return;
}

std::vector<char> kv_state_machine::snapshot() {
  // Your code here:
  return std::vector<char>();
}

void kv_state_machine::apply_snapshot(const std::vector<char> &snapshot) {
  // Your code here:
  return;
}
