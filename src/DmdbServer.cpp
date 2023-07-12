#include <signal.h>

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
#include "DmdbServerTerminateSignalHandler.hpp"



namespace Dmdb {

DmdbServer* DmdbServer::_self_instance = nullptr;

DmdbServer* DmdbServer::GetUniqueServerInstance(std::string &baseConfigFile) {
    if(_self_instance == nullptr){
        DmdbUtil::TrimString(baseConfigFile);
        _self_instance = new DmdbServer(baseConfigFile);
    }
    return _self_instance;
}

/* Child process like Rdb won't execute this function, if they finish their task, they will exit immediately,
 * won't enter while(1) loop */
void DmdbServer::ShutDownServerIfNeed() {
    if(_plan_to_shutdown) {
        _server_logger->WriteToServerLog(DmdbServerLogger::Verbosity::VERBOSE,
                                         "Server is shutting down");
        _rdb_manager->KillChildProcessIfAlive();

        bool isSaveOk = _rdb_manager->SaveData(-1, false) == SaveRetCode::SAVE_OK;
        if(!isSaveOk) {
            _server_logger->WriteToServerLog(DmdbServerLogger::Verbosity::VERBOSE,
                                            "Failed to save RDB data to disk");
        }

        _server_logger->WriteToServerLog(DmdbServerLogger::Verbosity::VERBOSE,
                                         "Bye bye!");
        // TODO: delete _cluster_manager;
        // delete _terminate_signal_handler;
        delete _base_config_file_loader;
        delete _client_manager;
        delete _database_manager;
        delete _server_logger;
        delete _repl_manager;
        delete _rdb_manager;
        delete _event_manager;
        exit(0);
    }
}

bool DmdbServer::IsMyselfChild() {
    /* If we have other child processes in the future, we also need to add checks like this */
    return _rdb_manager->IsMyselfRDBChild();
}

void DmdbServer::ShutdownServerBySignal(int sig) {
    std::string logContent;
    switch(sig) {
        /* If user presses "ctrl+c", both main process and child processes will receive SIGINT */
        case SIGINT: {
            logContent = "Received signal SIGINT!";
            break;
        }
        case SIGTERM: {
            logContent = "Received signal SIGTERM!";
            break;
        }
        /*
        default: {
            logContent = "Received signal, plan to shutdown";
            break;
        }
        */
    }

    if((_plan_to_shutdown && sig == SIGINT)) {
        _server_logger->WriteToServerLog(DmdbServerLogger::Verbosity::VERBOSE, "You insist exiting immediately!");
        _rdb_manager->RemoveRdbChildTmpFile();
        exit(1);
    }
    _server_logger->WriteToServerLog(DmdbServerLogger::Verbosity::VERBOSE, logContent.c_str());
    _plan_to_shutdown = true;
}

void DmdbServer::InitWithConfigFile() {
    std::unordered_map<std::string, std::vector<std::string>> parasMap;
    _base_config_file_loader->LoadConfigFile(parasMap);
    if(parasMap.find("password") != parasMap.end()) {
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
    if(parasMap.find("tcp_back_log") != parasMap.end()) {
        std::string strTCPBackLog = parasMap["tcp_back_log"][0];
        _tcp_back_log = atoi(strTCPBackLog.c_str());
        if(_tcp_back_log <= 0) {
            DmdbUtil::ServerExitWithErrMsg("Invalid tcp_back_log!");
        }
    }

    if(parasMap.find("is_daemonize") != parasMap.end()) {
        std::string strIsDaemonize = parasMap["is_daemonize"][0];
        bool isValid = DmdbUtil::GetBoolFromString(strIsDaemonize, _is_daemonize);
        if(!isValid)
            DmdbUtil::ServerExitWithErrMsg("Invalid is_daemonize!");
    }
    /*
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

        if(parasMap.find("is_preamble") == parasMap.end()) {
            _is_preamble = true;
        } else {
            std::string strIsAofUseRdbPreambleEnabled = parasMap["is_preamble"][0];
            bool isValid = DmdbUtil::GetBoolFromString(strIsAofUseRdbPreambleEnabled, _is_preamble);
            if(!isValid)
                DmdbUtil::ServerExitWithErrMsg("Invalid is_preamble!");
        }            
    }
    */ 
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

    if(parasMap.find("max_connection_num") != parasMap.end()) {
        std::string strMaxConnectionNum = parasMap["max_connection_num"][0];
        _max_connection_num = atoi(strMaxConnectionNum.c_str());
        if(_max_connection_num <= 0) {
            DmdbUtil::ServerExitWithErrMsg("Invalid max_connection_num!");
        }
    }
    /* 2 for : _epfd and _bind_ipv4_fd_for_client */
    _event_manager = DmdbEventManager::GetUniqueEventManagerInstance(_max_connection_num+2);

    if(parasMap.find("epoll_wait_timeout") != parasMap.end()) {
        std::string strTimeout = parasMap["epoll_wait_timeout"][0];
        int timeout = atoi(strTimeout.c_str());
        if(timeout <= 0) {
            DmdbUtil::ServerExitWithErrMsg("Invalid epoll_wait_timeout!");
        }
        _event_manager->SetEpollWaitTimeout(timeout);
    }

    if(parasMap.find("memory_max_available_size") != parasMap.end()) {
        std::string strMemoryMaxAvailableSize = parasMap["memory_max_available_size"][0];
        _memory_max_available_size = strtoull(strMemoryMaxAvailableSize.c_str(), nullptr, 10);
        if(errno == ERANGE || _memory_max_available_size <= 0) {
            DmdbUtil::ServerExitWithErrMsg("Invalid memory_max_available_size!");
        }
    }
    if(parasMap.find("is_cluster_mode") != parasMap.end()) {
        std::string strIsClusterMode = parasMap["is_cluster_mode"][0];
        bool isValid = DmdbUtil::GetBoolFromString(strIsClusterMode, _is_cluster_mode);
        if(!isValid)
            DmdbUtil::ServerExitWithErrMsg("Invalid is_cluster_mode!");
    }

    if(parasMap.find("expire_interval_ms") != parasMap.end()) {
        uint64_t expireIntervalMs = strtoull(parasMap["expire_interval_ms"][0].c_str(), nullptr, 10);
        if(errno == ERANGE) {
            DmdbUtil::ServerExitWithErrMsg("Invalid expire_interval_ms!");
        }
        _database_manager->SetExpireIntervalForDB(expireIntervalMs);
    }

    if(parasMap.find("is_master_role") != parasMap.end()) {
        std::string strIsMasterRole = parasMap["is_master_role"][0];
        bool isValid = DmdbUtil::GetBoolFromString(strIsMasterRole, _is_master_role);
        if(!isValid)
            DmdbUtil::ServerExitWithErrMsg("Invalid is_master_role!");
    }
    _repl_manager = DmdbReplicationManager::GenerateReplicationManagerByRole(_is_master_role);
    if(parasMap.find("full_sync_max_ms") != parasMap.end()) {
        uint64_t fullSyncMaxMs = strtoull(parasMap["full_sync_max_ms"][0].c_str(), nullptr, 10);
        if(errno == ERANGE || fullSyncMaxMs < 1000 || fullSyncMaxMs > 3600*1000) {
            DmdbUtil::ServerExitWithErrMsg("Invalid full_sync_max_ms!");
        }
        _repl_manager->SetFullSyncMaxMs(fullSyncMaxMs);
    }
    if(parasMap.find("repl_timely_task_interval") != parasMap.end()) {
        uint64_t timelyTaskInterval = strtoull(parasMap["repl_timely_task_interval"][0].c_str(), nullptr, 10);
        if(errno == ERANGE || timelyTaskInterval < 1000 || timelyTaskInterval > 60*1000) {
            DmdbUtil::ServerExitWithErrMsg("Invalid repl_timely_task_interval!");
        }
        _repl_manager->SetTaskInterval(timelyTaskInterval);
    }
    if(!_is_master_role) {
        if(parasMap.find("master_ip") == parasMap.end()) {
            DmdbUtil::ServerExitWithErrMsg("You must configure a master ip for this replica!");
        }
        if(parasMap.find("master_port_for_client") == parasMap.end()) {
            DmdbUtil::ServerExitWithErrMsg("You must configure a master port for this replica!");
        }
        if(parasMap.find("master_password") == parasMap.end()) {
            DmdbUtil::ServerExitWithErrMsg("You must configure a master password for this replica!");
        }
        std::string masterIp = parasMap["master_ip"][0];
        if(!DmdbUtil::IsValidIPV4Address(masterIp)) {
            DmdbUtil::ServerExitWithErrMsg("Invalid master_ip!");
        }
        std::string masterPortStr = parasMap["master_port_for_client"][0];
        int masterPort = atoi(masterPortStr.c_str());
        if(masterPort <= 0 || masterPort > 65535) {
            DmdbUtil::ServerExitWithErrMsg("Invalid master_port!");
        }
        _repl_manager->SetMasterAddrInfo(masterIp, masterPort);
        _repl_manager->SetMasterPassword(parasMap["master_password"][0]);
    }   
}


bool DmdbServer::LoadDataFromDisk() {
    bool isOk = _rdb_manager->LoadDatabase(-1);
    return isOk;
}


bool DmdbServer::StartServer() {
    srand(time(nullptr));
    SetupSignalHandler(DmdbServerTerminateSignalHandler::TerminateSignalHandle);
    if(_is_master_role)
        LoadDataFromDisk();
    else
        _repl_manager->FullSyncFromMater();
    if(!_client_manager->StartToListenIPV4())
        return false;
    return true;
}

void DmdbServer::SetupSignalHandler(void (*fun)(int)) {
    struct sigaction act;
    signal(SIGHUP, SIG_IGN);
    signal(SIGPIPE, SIG_IGN);
    sigemptyset(&act.sa_mask);
    act.sa_flags = 0;
    act.sa_handler = fun;
    sigaction(SIGTERM, &act, NULL);
    sigaction(SIGINT, &act, NULL);
}

void DmdbServer::ClearSignalHandler() {
    sigset_t clearSet;
    sigaddset(&clearSet, SIGINT);
    sigaddset(&clearSet, SIGTERM);
    sigemptyset(&clearSet);
    DmdbServerTerminateSignalHandler::SetServerInstance(nullptr);
}

DmdbServer::DmdbServer(std::string &baseConfigFile) {
    _server_version = SERVER_VERSION;
    _plan_to_shutdown = false;
    _is_preamble = false;
    _is_daemonize = false;
    _is_master_role = true;
    _memory_max_available_size = 3ull*1024ull*1024ull*1024ull;
    _is_cluster_mode = false;
    _max_connection_num = 1000;
    _server_connection_num = 0;
    _ipv4 = "";
    _tcp_back_log = 511;
    DmdbServerTerminateSignalHandler::SetServerInstance(this);
    _base_config_file_loader = new DmdbConfigFileLoader(baseConfigFile);
    _client_manager = DmdbClientManager::GetUniqueClientManagerInstance();
    _database_manager = new DmdbDatabaseManager();
    /* _server_logger, _event_manager, _rdb_manager, _repl_manager will be created in function InitWithConfigFile */
    InitWithConfigFile();
}

void DmdbServer::DoService() {
    StartServer();
    while(1) {
        _event_manager->WaitAndProcessEvents();
        _client_manager->ProcessClients();
        _repl_manager->TimelyTask();
        _database_manager->RemoveExpiredKeys();
        _rdb_manager->RdbCheckAndFinishJob();
        ShutDownServerIfNeed();
    }
    
}

DmdbServer::~DmdbServer() {
    delete _base_config_file_loader;
    delete _client_manager;
    delete _database_manager;
    delete _server_logger;
    delete _repl_manager;
    delete _rdb_manager;
    delete _event_manager;
}

#ifdef MAKE_TEST
void DmdbServer::PrintServerConfig() {
    std::cout << "Server Config:" << std::endl;
    std::cout << "  password: " << _client_manager->GetServerPassword() << std::endl;
    std::cout << "  tcp_back_log: " << _tcp_back_log << std::endl;
    std::cout << "  port_for_client: " << _client_manager->GetPortForClient() << std::endl;
    std::cout << "  client_timeout_seconds: " << _client_manager->GetTimeoutForClient() << std::endl;
    std::cout << "  client_input_buffer_max_size: " << _client_manager->GetInBufMaxSizeForClient() << std::endl;
    /*
    std::cout << "  is_daemonize: " << _is_daemonize << std::endl;
    std::cout << "  is_aof_enabled: " << _is_aof_enabled << std::endl;
    std::cout << "  aof_file: " << _aof_file << std::endl;
    std::cout << "  is_aof_use_rdb_preamble_enabled: " << _is_aof_use_rdb_preamble_enabled << std::endl;
    */
    std::cout << "  rdb_file: " << _rdb_manager->GetRDBFile() << std::endl;
    std::cout << "  is_master_role: " << _is_master_role << std::endl;
    std::cout << "  max_connection_num: " << _max_connection_num << std::endl;
    std::cout << "  memory_max_available_size: " << _memory_max_available_size << std::endl;
    std::cout << "  is_cluster_mode: " << _is_cluster_mode << std::endl;
}

#endif


}