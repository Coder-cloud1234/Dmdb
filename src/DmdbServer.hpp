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
struct DmdbRepilcationManagerRequiredComponents;

const uint8_t SERVER_VERSION = 1;

class DmdbServer {
public:
    static DmdbServer* GetUniqueServerInstance(std::string &baseConfigFile);
    static void ClearSignalHandler();
    void DoService();
    void ShutdownServerBySignal(int sig);
    ~DmdbServer();
    friend bool GetDmdbEventMangerRequiredComponents(DmdbEventMangerRequiredComponent &components);
    friend bool GetDmdbClientManagerRequiredComponent(DmdbClientManagerRequiredComponent &components);
    friend bool GetDmdbClientContactRequiredComponent(DmdbClientContactRequiredComponent &components);
    friend bool GetDmdbCommandRequiredComponents(DmdbCommandRequiredComponent &components);
    friend bool GetDmdbRDBRequiredComponents(DmdbRDBRequiredComponents &components);
    friend bool GetDmdbRepilcationManagerRequiredComponents(DmdbRepilcationManagerRequiredComponents &components);
private:
    DmdbServer(std::string &baseConfigfile);
    DmdbServer& operator=(const DmdbServer&);
    void InitWithConfigFile();
    bool LoadDataFromDisk();
    void ShutDownServerIfNeed();
    
    bool StartServer();
    void SetupSignalHandler(void (*fun)(int));
    bool IsMyselfChild();
    
    uint8_t _server_version;

    static DmdbServer* _self_instance;
    
    bool _plan_to_shutdown;
    
    bool _is_preamble;
    bool _is_daemonize;
    
    bool _is_master_role;
    
    unsigned long long _memory_max_available_size;
    bool _is_cluster_mode;
    DmdbConfigFileLoader* _base_config_file_loader;
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