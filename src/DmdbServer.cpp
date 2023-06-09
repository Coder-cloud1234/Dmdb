#include "DmdbServer.hpp"
#include "DmdbDatabaseManager.hpp"
#include "DmdbCommand.hpp"
#include "DmdbClientManager.hpp"
#include "DmdbReplicationManager.hpp"
#include "DmdbClusterManager.hpp"
#include "DmdbUtil.hpp"
#include "DmdbServerLogger.hpp"
#include "DmdbConfigFileLoader.hpp"
#include "DmdbEventManager.hpp"
#include "DmdbEventManagerCommon.hpp"
#include "DmdbRDBManager.hpp"

namespace Dmdb {

DmdbServer* DmdbServer::_self_instance = nullptr;

DmdbServer* DmdbServer::GetUniqueServerInstance(std::string &baseConfigFile) {
    if(_self_instance == nullptr){
        DmdbUtil::TrimString(baseConfigFile);
        _self_instance = new DmdbServer(baseConfigFile);
    }
    return _self_instance;
}

void DmdbServer::InitWithConfigFile() {
    std::unordered_map<std::string, std::vector<std::string>> parasMap;
    _base_config_file_loader->LoadConfigFile(parasMap);
    if(parasMap.find("password") == parasMap.end()) {
        _client_manager->SetPassword("123456");
    } else {
        _client_manager->SetPassword(parasMap["password"][0]);
    }
    if(parasMap.find("ipv4") == parasMap.end()) {
         DmdbUtil::ServerExitWithErrMsg("You must configure a IP4 address!");
    } else {
        std::string strIPV4 = parasMap["ipv4"][0];
        bool isValid = DmdbUtil::IsValidIPV4Address(strIPV4);
        if(!isValid) {
            DmdbUtil::ServerExitWithErrMsg("Invalid IP4 address!");
        }
        _ipv4 = strIPV4;
    }    
    if(parasMap.find("port_for_client") != parasMap.end()) {
        std::string strPort = parasMap["port_for_client"][0];
        int portForClient = atoi(strPort.c_str());
        if(portForClient <= 0 || portForClient > 65535) {
            DmdbUtil::ServerExitWithErrMsg("Invalid Port!");
        }
        _client_manager->SetPortForClient(portForClient);
    }
    if(parasMap.find("client_timeout_seconds") != parasMap.end()) {
        std::string strClientTimeoutSeconds = parasMap["client_timeout_seconds"][0];
        int clientTimeoutSeconds = atoi(strClientTimeoutSeconds.c_str());
        if(clientTimeoutSeconds < 0) {
            DmdbUtil::ServerExitWithErrMsg("Invalid client_timeout_seconds!");
        }
        _client_manager->SetTimeoutForClient(clientTimeoutSeconds);
    }
    if(parasMap.find("client_input_buffer_max_size") != parasMap.end()) {
        std::string strClientInputBufferMaxSize = parasMap["client_input_buffer_max_size"][0];
        size_t clientInputBufferMaxSize = strtoul(strClientInputBufferMaxSize.c_str(), nullptr, 10);
        if(clientInputBufferMaxSize <= 0) {
            DmdbUtil::ServerExitWithErrMsg("Invalid client_input_buffer_max_size!");
        }
        _client_manager->SetInBufMaxSizeForClient(clientInputBufferMaxSize);
    }
    if(parasMap.find("tcp_back_log") == parasMap.end()) {
        _tcp_back_log = 511;
    } else {
        std::string strTCPBackLog = parasMap["tcp_back_log"][0];
        _tcp_back_log = atoi(strTCPBackLog.c_str());
        if(_tcp_back_log <= 0) {
            DmdbUtil::ServerExitWithErrMsg("Invalid tcp_back_log!");
        }
    }

    if(parasMap.find("is_daemonize") == parasMap.end()) {
        _is_daemonize = false;
    } else {
        std::string strIsDaemonize = parasMap["is_daemonize"][0];
        bool isValid = DmdbUtil::GetBoolFromString(strIsDaemonize, _is_daemonize);
        if(!isValid)
            DmdbUtil::ServerExitWithErrMsg("Invalid is_daemonize!");
    }
    if(parasMap.find("is_aof_enabled") == parasMap.end()) {
        _is_aof_enabled = true;
    } else {
        std::string strIsAofEnabled = parasMap["is_aof_enabled"][0];
        bool isValid = DmdbUtil::GetBoolFromString(strIsAofEnabled, _is_aof_enabled);
        if(!isValid)
            DmdbUtil::ServerExitWithErrMsg("Invalid is_aof_enabled!");
    }
    if(_is_aof_enabled) {
        if(parasMap.find("aof_file") == parasMap.end()) {
            _aof_file = "Dmdb_AOF_File.aof";
        } else {
            _aof_file = parasMap["aof_file"][0];
        }

        if(parasMap.find("is_aof_use_rdb_preamble_enabled") == parasMap.end()) {
            _is_aof_use_rdb_preamble_enabled = true;
        } else {
            std::string strIsAofUseRdbPreambleEnabled = parasMap["is_aof_use_rdb_preamble_enabled"][0];
            bool isValid = DmdbUtil::GetBoolFromString(strIsAofUseRdbPreambleEnabled, _is_aof_use_rdb_preamble_enabled);
            if(!isValid)
                DmdbUtil::ServerExitWithErrMsg("Invalid is_aof_use_rdb_preamble_enabled!");
        }            
    } 
    if(parasMap.find("rdb_file") == parasMap.end()) {
        _rdb_manager =  DmdbRDBManager::GetUniqueRDBManagerInstance("Dmdb_RDB_File.rdb");
    } else {
        _rdb_manager = DmdbRDBManager::GetUniqueRDBManagerInstance(parasMap["rdb_file"][0]);
    }
    if(parasMap.find("server_log_file") == parasMap.end()) {
        // _server_log_file = "Server_Log_File.log";
        _server_logger = DmdbServerLogger::GetUniqueServerLogger("Server_Log_File.log", DmdbServerLogger::Verbosity::VERBOSE);
    } else {
        // _server_log_file = parasMap["server_log_file"][0];
        if(parasMap.find("server_log_verbose") == parasMap.end()) {
            _server_logger = DmdbServerLogger::GetUniqueServerLogger(parasMap["server_log_file"][0], DmdbServerLogger::Verbosity::VERBOSE);
        } else {
            std::string strVerbosity = parasMap["server_log_verbose"][0];
            DmdbServerLogger::Verbosity verbosity = DmdbServerLogger::Verbosity::VERBOSE;
            bool isValid = DmdbServerLogger::String2Verboity(strVerbosity, verbosity);
            if(!isValid) {
                DmdbUtil::ServerExitWithErrMsg("Invalid server_log_verbose");
            }
            _server_logger = DmdbServerLogger::GetUniqueServerLogger(parasMap["server_log_file"][0], verbosity);
        }
    }
    /*
    if(parasMap.find("is_sys_log_enabled") == parasMap.end()) {
        _is_sys_log_enabled = false;
         
    } else {
        std::string strIsSysLogEnabled = parasMap["is_sys_log_enabled"][0];
        bool isValid = DmdbUtil::GetBoolFromString(strIsSysLogEnabled, _is_sys_log_enabled);
        if(!isValid)
            DmdbUtil::ServerExitWithErrMsg("Invalid is_sys_log_enabled!");
    }
    */ 
    if(parasMap.find("is_master_role") == parasMap.end()) {
        _is_master_role = false;
    } else {
        std::string strIsMasterRole = parasMap["is_master_role"][0];
        bool isValid = DmdbUtil::GetBoolFromString(strIsMasterRole, _is_master_role);
        if(!isValid)
            DmdbUtil::ServerExitWithErrMsg("Invalid is_master_role!");
    }
    if(parasMap.find("max_connection_num") == parasMap.end()) {
        _max_connection_num = 10000;
    } else {
        std::string strMaxConnectionNum = parasMap["max_connection_num"][0];
        _max_connection_num = atoi(strMaxConnectionNum.c_str());
        if(_max_connection_num <= 0) {
            DmdbUtil::ServerExitWithErrMsg("Invalid max_connection_num!");
        }
    }
    /* 2 for : _epfd and _bind_ipv4_fd_for_client */
    _event_manager = DmdbEventManager::GetUniqueEventManagerInstance(_max_connection_num+2);

    if(parasMap.find("memory_max_available_size") == parasMap.end()) {
        _memory_max_available_size = 1024*1024*1024;
    } else {
        std::string strMemoryMaxAvailableSize = parasMap["memory_max_available_size"][0];
        _memory_max_available_size = strtoull(strMemoryMaxAvailableSize.c_str(), nullptr, 10);
        if(_memory_max_available_size <= 0) {
            DmdbUtil::ServerExitWithErrMsg("Invalid memory_max_available_size!");
        }
    }
    if(parasMap.find("is_cluster_mode") == parasMap.end()) {
        _is_cluster_mode = false;
    } else {
        std::string strIsClusterMode = parasMap["is_cluster_mode"][0];
        bool isValid = DmdbUtil::GetBoolFromString(strIsClusterMode, _is_cluster_mode);
        if(!isValid)
            DmdbUtil::ServerExitWithErrMsg("Invalid is_cluster_mode!");
    }       
}


bool DmdbServer::LoadDataFromDisk() {
    bool isOk = _rdb_manager->LoadDatabaseFromDisk();
    return isOk;
}


bool DmdbServer::StartServer() {
    LoadDataFromDisk();
    if(!_client_manager->StartToListenIPV4())
        return false;
    return true;
}

DmdbServer::DmdbServer(std::string &baseConfigFile) {
    _base_config_file_loader = new DmdbConfigFileLoader(baseConfigFile);
    _client_manager = DmdbClientManager::GetUniqueClientManagerInstance();
    _database_manager = new DmdbDatabaseManager();
    /* _server_logger, _event_manager, _rdb_manager will be created in function InitWithConfigFile */
    InitWithConfigFile();
}

void DmdbServer::DoService() {
    StartServer();
    while(1) {
        _event_manager->ProcessFiredEvents(100);
        _client_manager->ProcessClientsRequest();
        _client_manager->closeInvalidClients();
    }
    
}

#ifdef MAKE_TEST
void DmdbServer::PrintServerConfig() {
    std::cout << "Server Config:" << std::endl;
    std::cout << "  password: " << _client_manager->GetServerPassword() << std::endl;
    std::cout << "  tcp_back_log: " << _tcp_back_log << std::endl;
    std::cout << "  port_for_client: " << _client_manager->GetPortForClient() << std::endl;
    std::cout << "  client_timeout_seconds: " << _client_manager->GetTimeoutForClient() << std::endl;
    std::cout << "  client_input_buffer_max_size: " << _client_manager->GetInBufMaxSizeForClient() << std::endl;
    std::cout << "  is_daemonize: " << _is_daemonize << std::endl;
    std::cout << "  is_aof_enabled: " << _is_aof_enabled << std::endl;
    std::cout << "  aof_file: " << _aof_file << std::endl;
    std::cout << "  is_aof_use_rdb_preamble_enabled: " << _is_aof_use_rdb_preamble_enabled << std::endl;
    std::cout << "  rdb_file: " << _rdb_manager->GetRDBFile() << std::endl;
    std::cout << "  is_master_role: " << _is_master_role << std::endl;
    std::cout << "  max_connection_num: " << _max_connection_num << std::endl;
    std::cout << "  memory_max_available_size: " << _memory_max_available_size << std::endl;
    std::cout << "  is_cluster_mode: " << _is_cluster_mode << std::endl;
}

#endif


}