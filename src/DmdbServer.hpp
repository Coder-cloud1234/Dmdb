#include <unistd.h>

#include <ctime>
#include <iostream>
#include <vector>
#include <unordered_map>


#include "DmdbDatabaseManager.hpp"
#include "DmdbCommand.hpp"
#include "DmdbClientManager.hpp"
#include "DmdbCommon.hpp"
#include "DmdbReplicationManager.hpp"
#include "DmdbClusterManager.hpp"

#pragma once


namespace Dmdb {
class DmdbServer {
public:
    static DmdbServer* GetUniqueServerInstance();


private:
    DmdbServer();
    DmdbServer& operator=(const DmdbServer&);

    static DmdbServer* _self_instance;
    pid_t _self_pid;
    std::string _base_config_file;
    std::string _self_executable_file;
    std::vector<std::string> _self_exe_argv;
    std::string _self_pid_file;
    DmdbDatabaseManager _database_manager;
    std::unordered_map<std::string, DmdbCommand*> _dmdb_commands;
    std::string _password;
    size_t _self_memory_cost_init;
    int _port_for_client;
    int _port_for_cluster;
    int _bind_ip_fd_for_client;
    int _bind_ip_fd_for_cluster;
    int _tcp_back_log;
    std::unordered_map<std::string, DmdbClientManager> _dmdb_client_managers_map;
    bool _is_clients_paused;
    long _clients_pause_end_time;
    bool _is_loading_db_file;
    time_t _server_start_time;
    long long _processed_commands_num;
    long long _received_connections_num;
    long long _rejected_connections_num;
    long long _expired_keys_num;
    long long _keys_hit_num;
    long long _keys_miss_num;
    size_t _used_peak_memory_size;
    long long _full_sync_num;
    long long _partial_sync_ok_num;
    long long _partial_sync_failed_num;
    long long _net_input_size;
    long long _net_output_size;
    Verbosity _log_verbosity;
    int _client_timeout_seconds;
    int _tcp_keep_alive;
    int _dynamic_expire_enabled;
    size_t _client_input_buffer_max_size;
    bool _is_daemonize;
    bool _is_aof_enabled;
    std::string _aof_file;
    int _aof_fd;
    pid_t _aof_child_pid;
    std::string _aof_buf_for_main_process;
    time_t _aof_rewrite_start_time;
    time_t _last_aof_rewrite_time_cost;
    int _last_aof_bg_rewrite_status;
    bool _is_aof_use_rdb_preamble_enabled;
    pid_t _rdb_child_pid;
    std::string _rdb_file;
    time_t _last_rdb_save_ok_time;
    time_t _last_bgsave_start_time;
    bool _is_last_bgsave_ok;
    std::string _server_log_file;
    bool _is_sys_log_enabled;
    bool is_master_role;
    DmdbReplicationManager* _repl_manager;
    unsigned int _clients_max_num;
    unsigned long long _memory_max_available_size;
    bool _is_cluster_mode;
    DmdbClusterManager* _cluster_manager;
};
}