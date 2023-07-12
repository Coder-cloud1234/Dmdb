#include "DmdbEventProcessor.hpp"
#include "DmdbClientManager.hpp"
#include "DmdbEventManagerCommon.hpp"
#include "DmdbServerLogger.hpp"
#include "DmdbServerFriends.hpp"

namespace Dmdb {

bool DmdbAcceptEventProcessor::ProcessOneReadable() {
    struct epoll_event event = GetEvent();
    if(event.data.fd > 0) {
        struct sockaddr_in clientAddress;
        socklen_t clientAddressLength = sizeof(clientAddress);
#ifdef MAKE_TEST
        int flags = fcntl(event.data.fd, F_GETFL);
        if(!(flags & O_NONBLOCK)) {
            std::cout << "Listened fd isn't non-block" << std::endl;
        } 
#endif
        DmdbEventMangerRequiredComponent requiredComponents;
        GetDmdbEventMangerRequiredComponents(requiredComponents); 
        int fd = accept(event.data.fd, (struct sockaddr*)&clientAddress, &clientAddressLength);
        if(fd < 0) {
            if(errno != EAGAIN && errno != EWOULDBLOCK)
                requiredComponents._required_server_logger->WriteToServerLog(DmdbServerLogger::Verbosity::WARNING, 
                                                                             "Failed to accept: %s", strerror(errno));
            return false;
        }
        (*requiredComponents._required_server_connection_num)++;
        struct sockaddr_in *clientAddress2 = (struct sockaddr_in *)&clientAddress;
        char ipArray[100] = {0};
        std::string ip = inet_ntop(AF_INET,(void*)&(clientAddress2->sin_addr),ipArray,sizeof(ipArray));
        int port = ntohs(clientAddress2->sin_port);
        /* TODO: create a client using ip and port or cluster node.
           How to judge it is connection from cluster node or client:
           by fd == ? Then add event for fd*/
        if(event.data.fd == requiredComponents._required_client_manager->GetListenedIPV4Fd())
            requiredComponents._required_client_manager->HandleConnForClient(fd, ip, port);

        if((*requiredComponents._required_server_connection_num) > requiredComponents._required_server_max_conn_num) {
            
            requiredComponents._required_server_logger->WriteToServerLog(DmdbServerLogger::Verbosity::VERBOSE, 
                                                                "Refused a connection for the current num:%hu > max connection num:%hu",
                                                                (*requiredComponents._required_server_connection_num),
                                                                requiredComponents._required_server_max_conn_num);
            std::string replyMsg = "-ERR max number of clients reached\r\n";
            bool isWriteRight = ReplyRightNow(fd, replyMsg);
#ifdef MAKE_TEST
            if(isWriteRight == false) {
                std::cout << "Write err info to client wrongly" << std::endl;
            }
#endif
            if(event.data.fd == requiredComponents._required_client_manager->GetListenedIPV4Fd())
                requiredComponents._required_client_manager->DisconnectClient(fd);
            return false;
        }
#ifdef MAKE_TEST
        std::cout << "Accepted ip: " << ip << ", port: " << port << std::endl;
#endif
        requiredComponents._required_server_logger->WriteToServerLog(DmdbServerLogger::Verbosity::VERBOSE, 
                                                            "Accepted connection from ip:%s, port:%d", 
                                                            ip.c_str(), 
                                                            port);              
        return true;
    }
    return false;
}

void DmdbAcceptEventProcessor::ProcessReadable() {
    DmdbEventMangerRequiredComponent requiredComponents;
    GetDmdbEventMangerRequiredComponents(requiredComponents); 
    uint16_t maxProcessAccept = requiredComponents._required_server_max_conn_num;
    while(maxProcessAccept) {
        if(ProcessOneReadable() != true) {
            break;
        }
        maxProcessAccept--;
    }
}

void DmdbAcceptEventProcessor::ProcessWritable() {
    
}

DmdbAcceptEventProcessor::~DmdbAcceptEventProcessor() {

}

DmdbAcceptEventProcessor::DmdbAcceptEventProcessor(int fd, EpollEvent event) : DmdbEventProcessor(fd, event){
    SetFdNonBlocking(fd);
}



bool DmdbInteractEventProcessor::ProcessOneReadable() {
    DmdbEventMangerRequiredComponent requiredComponents;
    GetDmdbEventMangerRequiredComponents(requiredComponents); 
    struct epoll_event event = GetEvent();
    char buf[1024] = {0};
    /* We must leave 1 byte for 0 to mark the end of a string */
    int ret = read(event.data.fd, buf, sizeof(buf)-1);
    if(ret < 0) {
        if(errno != EAGAIN && errno != EWOULDBLOCK) {
            requiredComponents._required_server_logger->WriteToServerLog(DmdbServerLogger::Verbosity::WARNING,
                                                                "Failed to read data from fd: %d, error info: %s",
                                                                event.data.fd, strerror(errno));
            requiredComponents._required_client_manager->DisconnectClient(event.data.fd);            
        }
        return false;
    } else if(ret == 0) {
        requiredComponents._required_server_logger->WriteToServerLog(DmdbServerLogger::Verbosity::VERBOSE,
                                                                     "Client: %s closed the connection",
                                                                     requiredComponents._required_client_manager->GetNameOfClient(event.data.fd).c_str());
        requiredComponents._required_client_manager->DisconnectClient(event.data.fd);
        return false;
    } else {
        return requiredComponents._required_client_manager->AppendDataToClientInputBuf(event.data.fd, buf);
    }
}

void DmdbInteractEventProcessor::ProcessReadable() {
    while(ProcessOneReadable() != false);
}

void DmdbInteractEventProcessor::ProcessWritable() {
    struct epoll_event event = GetEvent();
    size_t bufLen = 0, writePos = 0;
    DmdbEventMangerRequiredComponent requiredComponents;
    GetDmdbEventMangerRequiredComponents(requiredComponents); 
    const char* clientOutputBuf = requiredComponents._required_client_manager->GetClientOutputBuf(event.data.fd, bufLen);
    int ret = 0;
    while(writePos < bufLen) {
        ret = write(event.data.fd, clientOutputBuf+writePos, bufLen);
        if(ret == 0) {
            break;
        } else if(ret < 0) {
            requiredComponents._required_server_logger->WriteToServerLog(DmdbServerLogger::Verbosity::WARNING, 
                                                                "Failed to write data for fd: %d, error info: %s",
                                                                event.data.fd, strerror(errno));
            if(errno != EAGAIN && errno != EWOULDBLOCK) {
                requiredComponents._required_client_manager->DisconnectClient(event.data.fd);
            }
            return;                                       
        } else {
            writePos += ret;
        }
    }
    requiredComponents._required_client_manager->HandleClientAfterWritting(event.data.fd, writePos);
}


DmdbInteractEventProcessor::DmdbInteractEventProcessor(int fd, EpollEvent event) : DmdbEventProcessor(fd, event){
    SetFdNonBlocking(fd);
    SetFdNoDelay(fd);
}

DmdbInteractEventProcessor::~DmdbInteractEventProcessor() {

}



DmdbEventProcessor::~DmdbEventProcessor() {

}

DmdbEventProcessor::DmdbEventProcessor(int fd, EpollEvent event) {
    _event.data.fd = fd;
    _event.events = 0;
    if(static_cast<uint32_t>(event) & static_cast<uint32_t>(EpollEvent::IN))
        _event.events |= EPOLLIN;
    if(static_cast<uint32_t>(event) & static_cast<uint32_t>(EpollEvent::OUT))
        _event.events |= EPOLLOUT;
}

struct epoll_event& DmdbEventProcessor::GetEvent() {
    return _event;
}

bool DmdbEventProcessor::SetFdNonBlocking(int fd) {
    int oldOption = fcntl(fd, F_GETFL);
    int ret = fcntl(fd, F_SETFL, oldOption|O_NONBLOCK);
    if(ret == -1) {
        DmdbEventMangerRequiredComponent requiredComponents;
        GetDmdbEventMangerRequiredComponents(requiredComponents); 
        requiredComponents._required_server_logger->WriteToServerLog(DmdbServerLogger::Verbosity::WARNING,
                                                            "Failed to set fd: %d to O_NONBLOCK! Error info: %s",
                                                            fd, strerror(errno));
        return false;
    }
    return true;
}

bool DmdbEventProcessor::SetFdNoDelay(int fd) {
    int val = 1;
    if(setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &val, sizeof(val)) != -1) {
        return true;
    } else {
        DmdbEventMangerRequiredComponent requiredComponents;
        GetDmdbEventMangerRequiredComponents(requiredComponents); 
        requiredComponents._required_server_logger->WriteToServerLog(DmdbServerLogger::Verbosity::WARNING,
                                                            "Failed to set fd: %d to TCP_NODELAY! Error info: %s",
                                                            fd, strerror(errno));        
        return false;
    }
}

bool DmdbEventProcessor::ReplyRightNow(int fd, const std::string &replyMsg) {
    SetFdNoDelay(fd);
    int ret = write(fd, replyMsg.c_str(), replyMsg.length());
    if(ret < 0 || static_cast<size_t>(ret) != replyMsg.length()) {
        return false;
    }
    return true;
}

}