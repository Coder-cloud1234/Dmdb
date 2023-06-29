#pragma once

#include <string>

#include "DmdbReplicationManager.hpp"


namespace Dmdb {

class DmdbClientContact;

class DmdbReplicaReplicationManager : public DmdbReplicationManager {
public:
    virtual DmdbClientContact* GetMasterClientContact();
    virtual void ReplicateDataToSlaves(const std::string &data);
    virtual bool IsOneOfMySlaves(DmdbClientContact* client);
    virtual bool IsMyMaster(const std::string clientName);
    virtual void SetMyMasterClientContact(DmdbClientContact* master);
    virtual void SetReplayOkSize(size_t addLen);
    virtual long long GetReplayOkSize();
    virtual bool RemoveMasterOrReplica(DmdbClientContact* client);
    virtual bool FullSyncDataToReplica(DmdbClientContact* client);
    virtual bool HandleFullSyncOver(int fd, bool isSuccess, bool isDisconnected);
    virtual void AddReplicaByFd(int fd);
    virtual bool FullSyncFromMater();
    virtual void ReportToMasterMyReplayOkSize();
    virtual void AskReplicaForReplayOkSize(DmdbClientContact* replica);
    virtual void SetReplicaReplayOkSize(DmdbClientContact* replica, long long size);
    virtual long long GetReplOffset();
    virtual std::string GetMultiBulkOfReplicasOrMaster();
    virtual void SetMasterPassword(const std::string &pwd);
    virtual void SetMasterAddrInfo(const std::string &ip, int port);
    virtual size_t GetReplicaCount();
    virtual size_t CountNumOfReplicasByOffset();
    virtual void WaitForNReplicasAck(DmdbClientContact* client, size_t waitNum, uint64_t waitMs);
    virtual bool StopWaitting(DmdbClientContact* client); 
    virtual bool IsWaitting(DmdbClientContact* client);
    void HandleSignal(int sig);
    void TimelyTask();
    DmdbReplicaReplicationManager();
    ~DmdbReplicaReplicationManager();
private:
    std::string SendCommandToMasterAndCheck(const std::string &command, const std::string &commandName, int socket, const std::string &expectReply, bool isCheck);
    void SetupTimer();
    void ClearTimer();
    std::string _master_password;
    // std::string _master_hostname;
    std::string _master_ip;
    int _master_port_for_client;

    DmdbClientContact *_current_master;

    long long _repl_ok_size;
};

}