#include "DmdbServerFriends.hpp"
#include "DmdbEventManagerCommon.hpp"
#include "DmdbClientManager.hpp"
#include "DmdbClientContact.hpp"
#include "DmdbServer.hpp"
#include "DmdbCommand.hpp"
#include "DmdbRDBManager.hpp"
#include "DmdbReplicationManager.hpp"

namespace Dmdb {
extern DmdbServer* serverInstance;

/* Before using these functions, we should make sure some components are not null */
bool GetDmdbEventMangerRequiredComponents(DmdbEventMangerRequiredComponent &components) {
    if(serverInstance == nullptr  || serverInstance->_server_logger == nullptr || serverInstance->_client_manager == nullptr)
        return false;
    components._required_server_logger = serverInstance->_server_logger;
    components._required_client_manager = serverInstance->_client_manager;
    components._required_server_max_conn_num = serverInstance->_max_connection_num;
    components._required_server_connection_num = &serverInstance->_server_connection_num;
    return true;
}

bool GetDmdbClientManagerRequiredComponent(DmdbClientManagerRequiredComponent &components) {
    if(serverInstance == nullptr || serverInstance->_server_logger == nullptr || 
       serverInstance->_event_manager == nullptr || serverInstance->_repl_manager == nullptr)
        return false;
    components._server_logger = serverInstance->_server_logger;
    components._event_manager = serverInstance->_event_manager;
    components._repl_manager = serverInstance->_repl_manager;
    components._server_ipv4 = serverInstance->_ipv4;
    components._server_tcp_backlog = serverInstance->_tcp_back_log;
    return true;
}

bool GetDmdbClientContactRequiredComponent(DmdbClientContactRequiredComponent &components) {
    if(serverInstance == nullptr || serverInstance->_server_logger == nullptr || 
       serverInstance->_client_manager == nullptr || serverInstance->_repl_manager == nullptr)
        return false;
    components._server_logger = serverInstance->_server_logger;
    components._client_manager = serverInstance->_client_manager;
    components._repl_manager = serverInstance->_repl_manager;
    components._is_myself_master = serverInstance->_is_master_role;
    return true;    
}

bool GetDmdbCommandRequiredComponents(DmdbCommandRequiredComponent &components) {
    if(serverInstance == nullptr || serverInstance->_client_manager == nullptr ||
       serverInstance->_database_manager == nullptr || serverInstance->_rdb_manager == nullptr ||
       serverInstance->_repl_manager == nullptr) {
        return false;
    }
    components._server_client_manager = serverInstance->_client_manager;
    components._server_database_manager = serverInstance->_database_manager;
    components._server_rdb_manager = serverInstance->_rdb_manager;
    components._repl_manager = serverInstance->_repl_manager;
    components._is_myself_master = serverInstance->_is_master_role;
    components._is_plan_to_shutdown = &serverInstance->_plan_to_shutdown;
    return true;
}

bool GetDmdbRDBRequiredComponents(DmdbRDBRequiredComponents &components) {
    if(serverInstance == nullptr || serverInstance->_server_logger == nullptr ||
       serverInstance->_database_manager == nullptr || serverInstance->_client_manager == nullptr ||
       serverInstance->_repl_manager == nullptr)
        return false;
    components._server_logger = serverInstance->_server_logger;
    components._database_manager = serverInstance->_database_manager;
    components._client_manager = serverInstance->_client_manager;
    components._repl_manager = serverInstance->_repl_manager;
    components._is_preamble = &serverInstance->_is_preamble;
    components._server_version = serverInstance->_server_version;
    
    return true;
}

bool GetDmdbRepilcationManagerRequiredComponents(DmdbRepilcationManagerRequiredComponents &components) {
    if(serverInstance == nullptr || serverInstance->_client_manager == nullptr ||
       serverInstance->_rdb_manager == nullptr || serverInstance->_event_manager == nullptr ||
       serverInstance->_server_logger == nullptr) {
        return false;
    }
    components._is_myself_master = serverInstance->_is_master_role;
    components._client_manager = serverInstance->_client_manager;
    components._rdb_manager = serverInstance->_rdb_manager;
    components._event_manager = serverInstance->_event_manager;
    components._server_logger = serverInstance->_server_logger;
    return true;
}

}