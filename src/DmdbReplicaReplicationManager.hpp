#include <string>

#include "DmdbReplicationManager.hpp"
#include "DmdbClientManager.hpp"

#pragma once

namespace Dmdb {

class DmdbReplicaReplicationManager : public DmdbReplicationManager {

private:
    std::string _master_password;
    std::string _master_hostname;
    int _master_port_for_client;
    int _repl_timeout;
    DmdbClientManager *_current_master;
    DmdbClientManager *_last_master;
    int _repl_status;
    int _repl_sync_socket_fd;
    int _repl_sync_tmp_file_fd;
    std::string repl_transfer_tmpfile;
    time_t _repl_down_start_time;
    std::string _master_replication_d;
};

}