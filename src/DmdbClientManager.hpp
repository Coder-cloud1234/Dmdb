#pragma once

#include <stddef.h>

#include <string>
#include <unordered_map>
#include <list>

namespace Dmdb {

class DmdbEventManager;
class DmdbClientContact;
class DmdbServerLogger;
/* All these members matches a member of DmdbServer, they may be necessary for 
 * DmdbClientManger's operations, the pointer members can affect DmdbServer's 
 * members */
struct DmdbClientManagerRequiredComponent {
    DmdbEventManager *_event_manager;
    DmdbServerLogger *_server_logger;
    std::string _server_ipv4;
    int _server_tcp_backlog;

};

class DmdbClientManager{

public:
    void HandleConnForClient(int fd, const std::string &ip, int port);
    void DisconnectClient(int fd);
    bool DisconnectClientByName(const std::string &name);
    DmdbClientContact* GetClientContactByName(const std::string &name);
    static DmdbClientManager* GetUniqueClientManagerInstance();
    bool AppendDataToClientInputBuf(int fd, char* data);
    const char* GetClientOutputBuf(int fd, size_t &bufLen);
    void HandleClientAfterWritting(int fd, size_t writedLen);
    int GetListenedIPV4Fd();
    bool StartToListenIPV4();
    void SetPortForClient(int portForClient);
    void SetTimeoutForClient(int timeoutSeconds);
    void SetInBufMaxSizeForClient(size_t inBufMaxSize);
    void SetPassword(const std::string &password);
    std::string GetNameOfClient(int fd);
    int GetPortForClient();
    int GetTimeoutForClient();
    size_t GetInBufMaxSizeForClient();
    size_t ProcessClientsRequest();
    size_t closeInvalidClients();
    bool AreClientsPaused();
    std::string GetServerPassword();
    bool PauseClients(uint64_t pauseMs);
    bool UnpauseClients();
    ~DmdbClientManager();
private:
    DmdbClientManager();
    bool DelIfExistsInToClose(int fd);
    bool InsertToCloseIfNotExists(DmdbClientContact* clientContact);
    static DmdbClientManager* _client_manager_instance;
    //std::unordered_map<std::string, DmdbClientContact*> _client_contacts;
    std::unordered_map<int, DmdbClientContact*> _fd_client_map;
    size_t _client_input_buf_max_size;
    int _port_for_client;
    int _bind_ipv4_fd_for_client;
    bool _is_clients_paused;
    uint64_t _clients_pause_end_time;
    int _client_timeout_seconds;
    std::list<DmdbClientContact*> _clients_to_close;
    std::string _password;
};

}