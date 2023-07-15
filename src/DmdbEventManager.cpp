#include "DmdbEventManager.hpp"
#include "DmdbClientManager.hpp"
#include "DmdbServerLogger.hpp"
#include "DmdbEventProcessor.hpp"
#include "DmdbEventManagerCommon.hpp"
#include "DmdbServerFriends.hpp"

namespace Dmdb {
DmdbEventManager* DmdbEventManager::_event_manager_instance = nullptr;

DmdbEventManager::DmdbEventManager(uint16_t maxNum) {
    _max_fd_num = maxNum;
    _fired_events = new epoll_event[_max_fd_num]{0};
    _is_inited = false;
    _epfd = -1;
    _epoll_wait_timeout = 100;
}

DmdbEventManager* DmdbEventManager::GetUniqueEventManagerInstance(uint16_t maxNum) {
    if(_event_manager_instance == nullptr) {
        _event_manager_instance = new DmdbEventManager(maxNum);
    }
    return _event_manager_instance;    
}

/* Before using DmdbEventManager, this function must be called and checked */
bool DmdbEventManager::InitEventManager() {
    if(_is_inited)
        return true;
    _epfd = epoll_create(1024);
    if(_epfd == -1) {
        DmdbEventMangerRequiredComponent requiredComponents;
        GetDmdbEventMangerRequiredComponents(requiredComponents);
        requiredComponents._required_server_logger->WriteToServerLog(DmdbServerLogger::Verbosity::WARNING, 
                                                                      "Failed to epoll_wait: %s", strerror(errno));
        return false;
    }
    _is_inited = true;
    return true;    
}

/* DmdbEventManager is not in charge of the fd created by other class,
 * it only releases the resource created by itself */
DmdbEventManager::~DmdbEventManager() {
    for(std::unordered_map<int, DmdbEventProcessor*>::iterator it = _fd_event_processor_map.begin(); it != _fd_event_processor_map.end(); ++it) {
        /* For closing fd, it will de finished by other components */
        delete it->second;
    }
    delete[] _fired_events;
    close(_epfd);
}

bool DmdbEventManager::AddEvent4Fd(int fd, EpollEvent event, EventProcessorType type) {
    InitEventManager();
    std::unordered_map<int, DmdbEventProcessor*>::iterator it = _fd_event_processor_map.find(fd);
    struct epoll_event epollEvent = {0};
    int ret;
    uint32_t eventMask = static_cast<uint32_t>(event);
    if(eventMask & static_cast<uint32_t>(EpollEvent::IN))
        epollEvent.events |= EPOLLIN;
    if(eventMask & static_cast<uint32_t>(EpollEvent::OUT))
        epollEvent.events |= EPOLLOUT;
    epollEvent.data.fd = fd;
    if(it == _fd_event_processor_map.end()) {
        if(_fd_event_processor_map.size() >= _max_fd_num)
            return false;
        ret = epoll_ctl(_epfd, EPOLL_CTL_ADD, fd, &epollEvent);
        if(ret == -1) {
            return false;
        }
        DmdbEventProcessor* eventProcessor;
        switch(type) {
            case EventProcessorType::ACCEPT_CONN: {
                eventProcessor = new DmdbAcceptEventProcessor(fd, event);
                break;
            }
            case EventProcessorType::INTERACT: {
                eventProcessor = new DmdbInteractEventProcessor(fd, event);
                break;
            }
        }
        eventProcessor->GetEvent() = epollEvent;
        _fd_event_processor_map[fd] = eventProcessor;
    } else {
        epollEvent.events |= _fd_event_processor_map[fd]->GetEvent().events;
        ret = epoll_ctl(_epfd, EPOLL_CTL_MOD, fd, &epollEvent);
        if(ret == -1) {
            return false;
        }    
        _fd_event_processor_map[fd]->GetEvent() = epollEvent;    
    }
    return true;
}

bool DmdbEventManager::DelFd(int fd, bool isConnection) {
    InitEventManager();
    std::unordered_map<int, DmdbEventProcessor*>::iterator it = _fd_event_processor_map.find(fd);
    if (it == _fd_event_processor_map.end())
        return false;
    epoll_event tmpEe;
    tmpEe.events = 0;
    tmpEe.data.fd = fd;
    epoll_ctl(_epfd,EPOLL_CTL_DEL,fd,&tmpEe);
    _fd_event_processor_map.erase(it);

    if(isConnection) {
        DmdbEventMangerRequiredComponent requiredComponents;
        GetDmdbEventMangerRequiredComponents(requiredComponents);    
        (*requiredComponents._required_server_connection_num)--;    
    }
    return true;
}

bool DmdbEventManager::DelEvent4Fd(int fd, EpollEvent event) {
    InitEventManager();
    std::unordered_map<int, DmdbEventProcessor*>::iterator it = _fd_event_processor_map.find(fd);
    if(it == _fd_event_processor_map.end())
        return false;
    if(event == EpollEvent::IN) {
        it->second->GetEvent().events &= ~(EPOLLIN);
    }
    if(event == EpollEvent::OUT) {
        it->second->GetEvent().events &= ~(EPOLLOUT);
    }
    epoll_ctl(_epfd,EPOLL_CTL_MOD,fd, &it->second->GetEvent());

    return true;
}

bool DmdbEventManager::ProcessFiredEvents(int timeout) {
    InitEventManager();
    int ret = epoll_wait(_epfd, _fired_events, _max_fd_num, timeout);
    if(ret < 0) {
        DmdbEventMangerRequiredComponent requiredComponents;
        GetDmdbEventMangerRequiredComponents(requiredComponents);    
        requiredComponents._required_server_logger->WriteToServerLog(DmdbServerLogger::Verbosity::WARNING,
                                                                      "Failed to epoll_wait: %s", 
                                                                      strerror(errno));
        return false;
    }
    for(int i = 0; i < ret; ++i) {
        std::unordered_map<int, DmdbEventProcessor*>::iterator it = _fd_event_processor_map.find(_fired_events[i].data.fd);
        if(it == _fd_event_processor_map.end()) {
            continue;
        }
        if(_fired_events[i].events & EPOLLIN) {
            _fd_event_processor_map[_fired_events[i].data.fd]->ProcessReadable();
        }
        /* Because it(iterator) may be erased by ProcessReadable() or ProcessWritable(), 
         * so we add this judgment to avoid null pointer of _fd_event_processor_map[] */
        if((it = _fd_event_processor_map.find(_fired_events[i].data.fd)) == _fd_event_processor_map.end()) {
            continue;
        }
        if(_fired_events[i].events & EPOLLOUT) {
            _fd_event_processor_map[_fired_events[i].data.fd]->ProcessWritable();
        }
    }
    memset(_fired_events, 0, sizeof(epoll_event)*ret);
    return true;
}

void DmdbEventManager::SetEpollWaitTimeout(int timeout) {
    _epoll_wait_timeout = timeout;
}

void DmdbEventManager::WaitAndProcessEvents() {
    ProcessFiredEvents(_epoll_wait_timeout);
}

}