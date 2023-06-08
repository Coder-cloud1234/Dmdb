#pragma once

#include <unistd.h>
#include <stdlib.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>

#include <ctime>
#include <iostream>
#include <vector>
#include <unordered_map>



//#include "config.hpp"

namespace Dmdb {

class DmdbConfigFileLoader;
class DmdbDatabaseManager;
class DmdbCommand;
class DmdbClientManager;
class DmdbServerLogger;
class DmdbReplicationManager;
class DmdbClusterManager;
class DmdbClientManager;
class DmdbEventManager;
class DmdbRDBManager;
struct DmdbEventMangerRequiredComponent;
struct DmdbClientManagerRequiredComponent;
struct DmdbClientContactRequiredComponent;
struct DmdbCommandRequiredComponent;
struct DmdbRDBRequiredComponents;

const uint8_t SERVER_VERSION = 1;

class DmdbServer {
public:
    static DmdbServer* GetUniqueServerInstance(std::string &baseConfigFile);
    void DoService();
#ifdef MAKE_TEST
    void PrintServerConfig();
#endif
    friend bool GetDmdbEventMangerRequiredComponents(DmdbEventMangerRequiredComponent &components);
    friend bool GetDmdbClientManagerRequiredComponent(DmdbClientManagerRequiredComponent &components);
    friend bool GetDmdbClientContactRequiredComponent(DmdbClientContactRequiredComponent &components);
    friend bool GetDmdbCommandRequiredComponents(DmdbCommandRequiredComponent &components);
    friend bool GetDmdbRDBRequiredComponents(DmdbRDBRequiredComponents &components);
private:
    DmdbServer(std::string &baseConfigfile);
    DmdbServer& operator=(const DmdbServer&);
    void InitWithConfigFile();
    bool LoadDataFromDisk();
    
    bool StartServer();
    
    uint8_t _server_version = SERVER_VERSION;

    static DmdbServer* _self_instance;
    pid_t _self_pid;
    //std::string _base_config_file;
    DmdbConfigFileLoader* _base_config_file_loader;
    std::string _self_executable_file;
    std::vector<std::string> _self_exe_argv;
    std::string _self_pid_file;
    
    std::unordered_map<std::string, DmdbCommand*> _dmdb_commands;
    
    size_t _self_memory_cost_init;
    
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
    
    bool _is_preamble;
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
    time_t _last_rdb_save_ok_time;
    time_t _last_bgsave_start_time;
    bool _is_last_bgsave_ok;
    
    // std::string _server_log_file;
    // bool _is_sys_log_enabled;
    bool _is_master_role;
    
    unsigned long long _memory_max_available_size;
    bool _is_cluster_mode;
    DmdbClusterManager* _cluster_manager;
    DmdbClientManager* _client_manager;
    DmdbDatabaseManager* _database_manager;
    DmdbServerLogger* _server_logger;
    DmdbReplicationManager* _repl_manager;
    DmdbEventManager* _event_manager;
    DmdbRDBManager* _rdb_manager;
    uint16_t _max_connection_num;
    uint16_t _server_connection_num;
    std::string _ipv4;
    int _tcp_back_log;
};
}