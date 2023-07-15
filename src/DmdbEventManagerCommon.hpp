#pragma once


#include <stdint.h>


namespace Dmdb {

class DmdbClientManager;
class DmdbServerLogger;

enum class EpollEvent {
    IN = 1,
    OUT = 2
};

/* All these members matches a member of DmdbServer, they may be necessary for 
 * DmdbEventManger's operations, the pointer members can affect DmdbServer's members */
struct DmdbEventMangerRequiredComponent {
    DmdbServerLogger* _required_server_logger;
    DmdbClientManager* _required_client_manager;
    /* This memeber differs from DmdbEventManager._max_fd_num:
     * This memeber includes client's num and cluster node's num while
     * DmdbEventManager._max_fd_num includes not only this member's num
     * but also the num of other fds, such as the fd that we are listening */
    uint16_t _required_server_max_conn_num;
    uint16_t* _required_server_connection_num;
};

}