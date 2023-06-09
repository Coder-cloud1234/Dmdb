#pragma once

#include <sys/epoll.h>
#include <unistd.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <sys/types.h>
#include <arpa/inet.h>

#include <unordered_map>



namespace Dmdb {

class DmdbEventProcessor;
enum class EventProcessorType;
enum class EpollEvent;

class DmdbEventManager {
public:

    static DmdbEventManager* GetUniqueEventManagerInstance(uint16_t maxNum);
    ~DmdbEventManager();
    
    /* If you already added a DmdbEventProcessor for fd, then type won't make sense */
    bool AddEvent4Fd(int fd, EpollEvent event, EventProcessorType type);
    bool DelEvent4Fd(int fd, EpollEvent event);
    bool DelFd(int fd, bool isConnection);
    bool ProcessFiredEvents(int timeout);
    
private:
    DmdbEventManager(uint16_t maxNum);
    bool InitEventManager();
    static DmdbEventManager* _event_manager_instance;
    std::unordered_map<int, DmdbEventProcessor*> _fd_event_processor_map;
    epoll_event* _fired_events;
    int _epfd;
    uint16_t _max_fd_num;
    bool _is_inited;
};

}