#include "DmdbClientManager.hpp"
#include "DmdbEventManager.hpp"
#include "DmdbClientContact.hpp"
#include "DmdbServerLogger.hpp"
#include "DmdbEventProcessor.hpp"
#include "DmdbEventManagerCommon.hpp"
#include "DmdbServerFriends.hpp"
#include "DmdbReplicationManager.hpp"

namespace Dmdb {

DmdbClientManager* DmdbClientManager::_client_manager_instance = nullptr;

/* The 3 functions below isn't necessary to be called before using DmdbClientManager actually */
void DmdbClientManager::SetPortForClient(int portForClient) {
    _port_for_client = portForClient;
}

void DmdbClientManager::SetTimeoutForClient(int timeoutSeconds) {
    _client_timeout_seconds = timeoutSeconds;
}

void DmdbClientManager::SetInBufMaxSizeForClient(size_t inBufMaxSize) {
    _client_input_buf_max_size = inBufMaxSize;
}

int DmdbClientManager::GetPortForClient() {
    return _port_for_client;
}

int DmdbClientManager::GetTimeoutForClient() {
    return _client_timeout_seconds;
}

size_t DmdbClientManager::GetInBufMaxSizeForClient() {
    return _client_input_buf_max_size;
}


bool DmdbClientManager::StartToListenIPV4() {
    int serverIPV4Sock = socket(PF_INET, SOCK_STREAM, 0);
    if(serverIPV4Sock < 0) {
        DmdbUtil::ServerHandleStartListenFailure(0);
        return false;
    }
    struct sockaddr_in address;
    bzero(&address, sizeof(address));
    address.sin_family = AF_INET;
    DmdbClientManagerRequiredComponent requiredComponents;
    GetDmdbClientManagerRequiredComponent(requiredComponents);    
    inet_pton(AF_INET, requiredComponents._server_ipv4.c_str(), &address.sin_addr);
    address.sin_port = htons(_port_for_client);
    int ret = bind(serverIPV4Sock, (struct sockaddr*)(&address), sizeof(address));
    if(ret == -1) {
        DmdbUtil::ServerHandleStartListenFailure(serverIPV4Sock);
        return false;
    }

    ret = listen(serverIPV4Sock, requiredComponents._server_tcp_backlog);
    if(ret == -1) {
        DmdbUtil::ServerHandleStartListenFailure(serverIPV4Sock);
        return false;
    }
    _bind_ipv4_fd_for_client = serverIPV4Sock;
    return requiredComponents._event_manager->AddEvent4Fd(_bind_ipv4_fd_for_client, 
                                                          Dmdb::EpollEvent::IN, 
                                                          Dmdb::EventProcessorType::ACCEPT_CONN);
}

DmdbClientManager::DmdbClientManager() {
    _port_for_client = 10000;
    _client_timeout_seconds = 1;
    _client_input_buf_max_size = 1024*1024*1024;
    _is_clients_paused = false;
    _clients_pause_end_time = 0;
    _password = "123456";
}


DmdbClientManager::~DmdbClientManager() {
    std::unordered_map<int, DmdbClientContact*>::iterator it;
    while((it = _fd_client_map.begin()) != _fd_client_map.end()) {
        DisconnectClient(it->first);
    }
    DmdbClientManagerRequiredComponent requiredComponents;
    GetDmdbClientManagerRequiredComponent(requiredComponents);
    requiredComponents._event_manager->DelFd(_bind_ipv4_fd_for_client, false);
    close(_bind_ipv4_fd_for_client);
}


DmdbClientManager* DmdbClientManager::GetUniqueClientManagerInstance() {
    if(_client_manager_instance == nullptr) {
        _client_manager_instance = new DmdbClientManager();
    }
    return _client_manager_instance;
}

int DmdbClientManager::GetListenedIPV4Fd() {
    return _bind_ipv4_fd_for_client;
}

void DmdbClientManager::HandleConnForClient(int fd, const std::string &ip, int port) {
    DmdbClientContact *clientContact = new DmdbClientContact(fd, ip, port);
    _fd_client_map[fd] = clientContact;
    DmdbClientManagerRequiredComponent requiredComponents;
    GetDmdbClientManagerRequiredComponent(requiredComponents);
    requiredComponents._event_manager->AddEvent4Fd(fd, EpollEvent::IN, EventProcessorType::INTERACT);
    requiredComponents._event_manager->AddEvent4Fd(fd, EpollEvent::OUT, EventProcessorType::INTERACT);
}

bool DmdbClientManager::DelIfExistsInToClose(int fd) {
    std::list<DmdbClientContact*>::iterator it = _clients_to_close.begin();
    while(it != _clients_to_close.end()) {
        if( (*it)->GetClientSocket() == fd ) {
            _clients_to_close.erase(it);
            return true;
        }
        it++;
    }
    return false;
}

bool DmdbClientManager::InsertToCloseIfNotExists(DmdbClientContact* clientContact) {
    std::list<DmdbClientContact*>::iterator it = _clients_to_close.begin();
    while(it != _clients_to_close.end()) {
        if((*it) == clientContact) {
            return false;
        }
        it++;
    }
    _clients_to_close.push_back(clientContact);
    return true;    
}

void DmdbClientManager::DisconnectClient(int fd) {
    DmdbClientManagerRequiredComponent requiredComponents;
    GetDmdbClientManagerRequiredComponent(requiredComponents);
    std::unordered_map<int, DmdbClientContact*>::iterator it = _fd_client_map.find(fd);
    /* We don't have to delete it from _clients_to_close. */
    if(it != _fd_client_map.end()) {
        requiredComponents._server_logger->WriteToServerLog(DmdbServerLogger::Verbosity::VERBOSE, 
                                                            "Close the connection with client: %s",
                                                            it->second->GetClientName());
        requiredComponents._event_manager->DelFd(it->first, true);
        close(it->first);
        DelIfExistsInToClose(it->first);
        /* RemoveMasterOrReplica will check whether it is a replica of mine */
        requiredComponents._repl_manager->RemoveMasterOrReplica(it->second);
        /* Remove the clientContact from _clients_wait_n_replicas of master instance if it is waitting, 
         * if it is not waitting, this function do nothing */
        requiredComponents._repl_manager->StopWaitting(it->second);
        delete it->second;
        _fd_client_map.erase(it);       
    }
}

bool DmdbClientManager::DisconnectClientByName(const std::string &name) {
    /* Here we get the client just by iterating the whole unordered_map<id, DmdbClientContact>, but
     * in the future, we will add an unordered_map<name, DmdbClientContact>, then
     * we can get DmdbClientContact by name in O(l) time cost*/
    std::unordered_map<int, DmdbClientContact*>::iterator it = _fd_client_map.begin();
    while(it != _fd_client_map.end()) {
        if(it->second->GetClientName() == name) {
            DisconnectClient(it->first);
            return true;
        }
        it++;
    }
    return false;
}

DmdbClientContact* DmdbClientManager::GetClientContactByName(const std::string &name) {
    /* Here we get the client just by iterating the whole unordered_map<id, DmdbClientContact>, but
     * in the future, we will add an unordered_map<name, DmdbClientContact>, then
     * we can get DmdbClientContact by name in O(l) time cost*/
    std::unordered_map<int, DmdbClientContact*>::iterator it = _fd_client_map.begin();
    while(it != _fd_client_map.end()) {
        if(it->second->GetClientName() == name) {
            return it->second;
        }
        it++;
    }
    return nullptr;
}


bool DmdbClientManager::AppendDataToClientInputBuf(int fd, char* data) {
    std::unordered_map<int, DmdbClientContact*>::iterator it = _fd_client_map.find(fd);
    if(it != _fd_client_map.end()) {
        if(it->second->GetInputBufLength()+strlen(data) > _client_input_buf_max_size ) {
            DmdbClientManagerRequiredComponent requiredComponents;
            GetDmdbClientManagerRequiredComponent(requiredComponents);
            requiredComponents._server_logger->WriteToServerLog(DmdbServerLogger::Verbosity::WARNING,
                                                                  "Close client for exceeding the input buffer max size: %d, the current size: %d, request data size: %d", 
                                                                  _client_input_buf_max_size, it->second->GetInputBufLength(), strlen(data));
            DisconnectClient(fd);
            return false;
        }
        it->second->AppendDataToInputBuf(data);
        return true;
    }
    return false;
}

const char* DmdbClientManager::GetClientOutputBuf(int fd, size_t &bufLen) {
    std::unordered_map<int, DmdbClientContact*>::iterator it = _fd_client_map.find(fd);
    if(it != _fd_client_map.end()) {
        bufLen = it->second->GetOutputBufLength();
        return it->second->GetOutputBuf();
    }
    return nullptr;
}

std::string DmdbClientManager::GetNameOfClient(int fd) {
    std::unordered_map<int, DmdbClientContact*>::iterator it = _fd_client_map.find(fd);
    if(it != _fd_client_map.end()) {
        return it->second->GetClientName();
    }
    return "";    
}

DmdbClientContact* DmdbClientManager::GetClientContactByFd(int fd) {
    std::unordered_map<int, DmdbClientContact*>::iterator it = _fd_client_map.find(fd);
    if(it != _fd_client_map.end()) {
        return it->second;
    }
    return nullptr;    
}

void DmdbClientManager::HandleClientAfterWritting(int fd, size_t writedLen) {
    std::unordered_map<int, DmdbClientContact*>::iterator it = _fd_client_map.find(fd);
    if(it != _fd_client_map.end()) {
        it->second->ClearRepliedData(writedLen);
    }
}

size_t DmdbClientManager::ProcessClientsRequest() {
    size_t processedNum = 0;
    DmdbClientManagerRequiredComponent requiredComponents;
    GetDmdbClientManagerRequiredComponent(requiredComponents);
    for(std::unordered_map<int, DmdbClientContact*>::iterator it = _fd_client_map.begin();
        it != _fd_client_map.end();
        it++) {
        /* we shouldn't pause replication from master */
        if(!_is_clients_paused || requiredComponents._repl_manager->IsMyMaster(it->second->GetClientName())) {
            if(it->second->GetStatus() & static_cast<uint32_t>(ClientStatus::CLOSE_AFTER_REPLY)) {
                InsertToCloseIfNotExists(it->second);
            } else {
                it->second->ProcessClientRequest();
            }
            processedNum++;
        } else {
            uint64_t currentMs = DmdbUtil::GetCurrentMs();
            if(currentMs >= static_cast<uint64_t>(_clients_pause_end_time)) {
                UnpauseClients();
            } else {
                return processedNum;
            }
        }
    }
    return processedNum;
}

bool DmdbClientManager::AreClientsPaused() {
    if(!_is_clients_paused)
        return false;
    uint64_t currentMs = DmdbUtil::GetCurrentMs();
    if (currentMs >= static_cast<uint64_t>(_clients_pause_end_time)) {
        UnpauseClients();
    }
    return _is_clients_paused;
}

size_t DmdbClientManager::CloseInvalidClients() {
    size_t closedNum = 0;
    std::list<DmdbClientContact*>::iterator it = _clients_to_close.begin();
    std::list<DmdbClientContact*>::iterator toDel = it;
    while(it != _clients_to_close.end()) {
        if((*it)->GetOutputBufLength() > 0) {
            it++;
            continue;
        }
        toDel = it;
        it++;
        DisconnectClient((*toDel)->GetClientSocket());
        closedNum++;
    }
    return closedNum;
}

void DmdbClientManager::ProcessClients() {
    ProcessClientsRequest();
    CloseInvalidClients();
}

void DmdbClientManager::SetPassword(const std::string &password) {
    _password = password;
}

std::string DmdbClientManager::GetServerPassword() {
    return _password;
}

bool DmdbClientManager::PauseClients(uint64_t pauseMs) {
    _is_clients_paused = true;
    _clients_pause_end_time = DmdbUtil::GetCurrentMs() + pauseMs;
    return true;
}

bool DmdbClientManager::UnpauseClients() {
    _is_clients_paused = false;
    _clients_pause_end_time = 0;
    return true;
}

}