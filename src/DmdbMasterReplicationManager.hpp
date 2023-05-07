#include <string>

#include "DmdbReplicationManager.hpp"

#pragma once

namespace Dmdb {
class DmdbMasterReplicationManager : public DmdbReplicationManager {

private:
    std::string _current_replication_id;
    std::string _last_replication_id;
    long long _current_repl_offset;
    long long _repl_offset_from_last_replid;
    int _ping_slave_interval_seconds;
    std::string _repl_buffer;
    long long _repl_buffer_size;
    long long _repl_buffer_used;
    long long _repl_buffer_to_write_pos;
    long long _buffer_pos_in_repl_offset;
    int _repl_slaves_min_num_to_write;
    int _repl_slaves_ok_num;
};
}