#pragma once

#include <sys/epoll.h>
#include <unistd.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <sys/types.h>
#include <arpa/inet.h>
#include <fcntl.h>
#include <netinet/tcp.h>

#include <string>


namespace Dmdb {

enum class EpollEvent;

enum class EventProcessorType {
    ACCEPT_CONN,
    INTERACT
};

class DmdbEventProcessor {
public:

    virtual void ProcessReadable() = 0;
    virtual void ProcessWritable() = 0;
    DmdbEventProcessor(int fd, EpollEvent event);
    virtual ~DmdbEventProcessor();
    struct epoll_event& GetEvent();
protected:
    bool SetFdNonBlocking(int fd);
    bool SetFdNoDelay(int fd);
    bool ReplyRightNow(int fd, const std::string &replyMsg);
private:
    struct epoll_event _event;
};

class DmdbAcceptEventProcessor : public DmdbEventProcessor {
public:
    DmdbAcceptEventProcessor(int fd, EpollEvent event);
    virtual void ProcessReadable();
    virtual void ProcessWritable();
    virtual ~DmdbAcceptEventProcessor();
private:
    bool ProcessOneReadable();
};

class DmdbInteractEventProcessor : public DmdbEventProcessor {
public:
    DmdbInteractEventProcessor(int fd, EpollEvent event);
    virtual void ProcessReadable();
    virtual void ProcessWritable();
    virtual ~DmdbInteractEventProcessor();
private:
    bool ProcessOneReadable();
};


}