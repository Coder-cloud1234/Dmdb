#include <unistd.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <signal.h> 


#include "DmdbReplicaReplicationManager.hpp"
#include "DmdbClientContact.hpp"
#include "DmdbUtil.hpp"
#include "DmdbServerFriends.hpp"
#include "DmdbRDBManager.hpp"
#include "DmdbServerLogger.hpp"
#include "DmdbTimerHandlerForBlockingIO.hpp"
#include "DmdbClientManager.hpp"

namespace Dmdb {

const size_t TMP_RECV_BUF_LEN = 1024;

DmdbClientContact* DmdbReplicaReplicationManager::GetMasterClientContact() {
    return _current_master;
}

void DmdbReplicaReplicationManager::ReplicateDataToSlaves(const std::string &data) {
    
}

bool DmdbReplicaReplicationManager::IsOneOfMySlaves(DmdbClientContact* client) {
    return false;
}

bool DmdbReplicaReplicationManager::IsMyMaster(const std::string clientName) {
    return clientName == (_master_ip + ":" + std::to_string(_master_port_for_client));
}

void DmdbReplicaReplicationManager::SetMyMasterClientContact(DmdbClientContact* master) {
    _current_master = master;
}

void DmdbReplicaReplicationManager::SetReplayOkSize(size_t addLen) {
    _repl_ok_size += addLen;
}

long long DmdbReplicaReplicationManager::GetReplayOkSize() {
    return _repl_ok_size;
}

bool DmdbReplicaReplicationManager::RemoveMasterOrReplica(DmdbClientContact* client) {
    if(client == _current_master) {
        _current_master = nullptr;
        // _repl_sync_socket_fd = -1;
        /* TODO: Broadcast to all the clients that master doesn't exist if necessary */
        return true;
    }
    return false;
}

bool DmdbReplicaReplicationManager::FullSyncDataToReplica(DmdbClientContact* client) {
    return false;
}

bool DmdbReplicaReplicationManager::HandleFullSyncOver(int fd, bool isSuccess, bool isDisconnected) {
    return false;
}

void DmdbReplicaReplicationManager::AddReplicaByFd(int fd) {

}

std::string DmdbReplicaReplicationManager::SendCommandToMasterAndCheck(const std::string &command, const std::string &commandName,
                                                                       int socket, const std::string &expectReply,
                                                                       bool isCheck = false) {
    DmdbRepilcationManagerRequiredComponents components;
    GetDmdbRepilcationManagerRequiredComponents(components);    
    char recvBuf[TMP_RECV_BUF_LEN] = {0};
    std::string actualReceiveStr = "";
    int ret = send(socket, command.c_str(), command.length(), 0);
    if(ret < 0) {
        components._server_logger->WriteToServerLog(DmdbServerLogger::Verbosity::WARNING,
                                                    (std::string("Failed to send to master, error info:") + std::string(strerror(errno))).c_str());
        exit(0);
    }
    if(!isCheck)
        return actualReceiveStr;
    /* Using this function we can ensure that we will only receive one row of data even though there is much more data in tcp buffer */
    ret = DmdbUtil::RecvLineFromSocket(socket, recvBuf, sizeof(recvBuf));
    // ret = recv(socket, recvBuf, sizeof(recvBuf), 0);
    if(ret < 0) {
        components._server_logger->WriteToServerLog(DmdbServerLogger::Verbosity::WARNING, 
                                                    ("Failed to receive data from master, error info:" + std::string(strerror(errno))).c_str());
        exit(0);
    }
    actualReceiveStr = recvBuf;
    if(actualReceiveStr.find(expectReply) == std::string::npos) {
        std::string errMsg = recvBuf;
        errMsg.substr(5, errMsg.length()-5);
        errMsg = "Failed to execute " + commandName + " in master, error info:" + errMsg;
        components._server_logger->WriteToServerLog(DmdbServerLogger::Verbosity::WARNING, errMsg.c_str());
        exit(0);
    }
    return actualReceiveStr;
}

void DmdbReplicaReplicationManager::HandleSignal(int sig) {
    DmdbRepilcationManagerRequiredComponents components;
    GetDmdbRepilcationManagerRequiredComponents(components);
    switch(sig) {
        case SIGALRM: {
            components._server_logger->WriteToServerLog(DmdbServerLogger::Verbosity::WARNING,
                                                        "It seems that the master doesn't response, we decide to exit");
            exit(0);
        }
    }
}

void DmdbReplicaReplicationManager::SetupTimer() {
    // DmdbTimerHandlerForBlockingIO<DmdbReplicaReplicationManager>::SetInstance(this);
    DmdbTimerHandlerForBlockingIO::SetInstance(this);
    // sighandler_t src_sig;  
    struct sigaction sa_alarm;
    sigemptyset(&sa_alarm.sa_mask);
    sa_alarm.sa_flags = SA_RESETHAND;
    // void (*fun)(int) = DmdbTimerHandlerForBlockingIO<DmdbReplicaReplicationManager>::HandleSignal;
    void (*fun)(int) = DmdbTimerHandlerForBlockingIO::HandleSignal;
    sa_alarm.sa_handler = fun;
    sigaction(SIGALRM, &sa_alarm, nullptr);
    alarm(_full_sync_max_ms/1000);
}

void DmdbReplicaReplicationManager::ClearTimer() {
    alarm(0);
    // DmdbTimerHandlerForBlockingIO<DmdbReplicaReplicationManager>::SetInstance(nullptr);
    DmdbTimerHandlerForBlockingIO::SetInstance(nullptr);
    sigset_t clearSet;
    sigaddset(&clearSet, SIGALRM);
    sigemptyset(&clearSet);
}

bool DmdbReplicaReplicationManager::FullSyncFromMater() {
    DmdbRepilcationManagerRequiredComponents components;
    GetDmdbRepilcationManagerRequiredComponents(components);
    
   	struct sockaddr_in serveraddr;
	serveraddr.sin_family = AF_INET;
	serveraddr.sin_port = htons(_master_port_for_client);
	serveraddr.sin_addr.s_addr = inet_addr(_master_ip.c_str());
    int socketWithMaster = socket(AF_INET, SOCK_STREAM, 0);
	if (socketWithMaster < 0) {
        components._server_logger->WriteToServerLog(DmdbServerLogger::Verbosity::WARNING, 
                                                    ("Failed to create socket to connect master, error info:" + std::string(strerror(errno))).c_str());
        exit(0);
        return false;
	}
    int ret = connect(socketWithMaster, (struct sockaddr *)&serveraddr, sizeof(struct sockaddr));
    if(ret < 0) {
        components._server_logger->WriteToServerLog(DmdbServerLogger::Verbosity::WARNING, 
                                                    ("Failed to connect master, error info:" + std::string(strerror(errno))).c_str());
        exit(0);
        return false;
    }

#ifndef MAKE_TEST
    /* We set this timer for setting a timeout for recving data from master, except disconnection, the master machine may be in an unresponsive state */
    SetupTimer();
#endif

    /* Send auth and sync command to master */
    std::string command = std::string("*2\r\n") + std::string("$4\r\n") + std::string("auth\r\n") + "$"+std::to_string(_master_password.length())+std::string("\r\n")+_master_password+"\r\n";
    SendCommandToMasterAndCheck(command, "auth", socketWithMaster, "OK", true);
    command = std::string("*1\r\n") + "$4\r\nsync\r\n";
    std::string recvStr = SendCommandToMasterAndCheck(command, "sync", socketWithMaster, "FULLRESYNC", true);
    recvStr = recvStr.substr(12, recvStr.length()-12);
    size_t expectOffset = strtoll(recvStr.c_str(), nullptr, 10);

    bool isReplSucc = components._rdb_manager->LoadDatabase(socketWithMaster);
    if(isReplSucc) {
        components._server_logger->WriteToServerLog(DmdbServerLogger::Verbosity::VERBOSE,
                                                    "Replicate RDB data from master(ip:%s, port:%d) successfully",
                                                    _master_ip.c_str(),
                                                    _master_port_for_client);
        /* This function will add event processor for socketWithMaster, when constructing event processor, it will set socketWithMaster non-blocking */
        components._client_manager->HandleConnForClient(socketWithMaster, _master_ip, _master_port_for_client);
        _current_master = components._client_manager->GetClientContactByFd(socketWithMaster);
        _current_master->SetChecked();
    } else {
        components._server_logger->WriteToServerLog(DmdbServerLogger::Verbosity::WARNING,
                                                    "Failed to replicate RDB data from master(ip:%s, port:%d)",
                                                    _master_ip.c_str(),
                                                    _master_port_for_client);
        exit(0);
    }

#ifndef MAKE_TEST
    /* If we receive data from master normally, we should cancel this timer */
    ClearTimer();
#endif

    /* Only receiving all the rdb data successfully, we set _repl_ok_size */
    _repl_ok_size = expectOffset;
    return isReplSucc;
}

void DmdbReplicaReplicationManager::AskReplicaForReplayOkSize(DmdbClientContact* replica) {

}

void DmdbReplicaReplicationManager::ReportToMasterMyReplayOkSize() {
    std::string command = "*3\r\n";
    command += "$8\r\nreplconf\r\n";
    command += "$3\r\nack\r\n";
    std::string replayOkSizeStr = std::to_string(_repl_ok_size);
    command += "$" + std::to_string(replayOkSizeStr.length()) + "\r\n" + replayOkSizeStr + "\r\n";
    if(_current_master != nullptr)
        _current_master->AddReplyData2Client(command);
}

DmdbReplicaReplicationManager::DmdbReplicaReplicationManager() {
    _full_sync_max_ms = 60*1000;
    _repl_timely_task_interval = 1000;
    _last_timely_exe_ms = 0;
    _master_password = "";
    _master_ip = "";
    _master_port_for_client = -1;
    _current_master = nullptr;
    _repl_ok_size = 0;
}

DmdbReplicaReplicationManager::~DmdbReplicaReplicationManager() {

}

void DmdbReplicaReplicationManager::SetMasterPassword(const std::string &pwd) {
    _master_password = pwd;
}

void DmdbReplicaReplicationManager::SetMasterAddrInfo(const std::string &ip, int port) {
    _master_ip = ip;
    _master_port_for_client = port;
}

void DmdbReplicaReplicationManager::SetReplicaReplayOkSize(DmdbClientContact* replica, long long size) {

}

long long DmdbReplicaReplicationManager::GetReplOffset() {
    return 0;
}

std::string DmdbReplicaReplicationManager::GetMultiBulkOfReplicasOrMaster() {
    std::string multiBulk = "*4\r\n";
    multiBulk += "$7\r\n";
    multiBulk += "replica\r\n";
    multiBulk += "$" + std::to_string(_master_ip.length()) + "\r\n";
    multiBulk += _master_ip + "\r\n";
    multiBulk += "$" + std::to_string(std::to_string(_master_port_for_client).length()) + "\r\n";
    multiBulk += std::to_string(_master_port_for_client) + "\r\n";
    multiBulk += "$" + std::to_string(std::to_string(GetReplayOkSize()).length()) + "\r\n";
    multiBulk += std::to_string(GetReplayOkSize()) + "\r\n";
    return multiBulk;
}

void DmdbReplicaReplicationManager::TimelyTask() {
    uint64_t currentMs = DmdbUtil::GetCurrentMs();
    if(currentMs - _last_timely_exe_ms < _repl_timely_task_interval) {
        return;
    }
    _last_timely_exe_ms = currentMs;
    if(_current_master != nullptr)
        ReportToMasterMyReplayOkSize();
}

size_t DmdbReplicaReplicationManager::GetReplicaCount() {
    return 0;
}

size_t DmdbReplicaReplicationManager::CountNumOfReplicasByOffset() {
    return 0;
}

void DmdbReplicaReplicationManager::WaitForNReplicasAck(DmdbClientContact* client, size_t waitNum, uint64_t waitMs) {

}

bool DmdbReplicaReplicationManager::StopWaitting(DmdbClientContact* client) {
    return true;
} 

bool DmdbReplicaReplicationManager::IsWaitting(DmdbClientContact* client) {
    return false;
}

}
