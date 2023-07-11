#pragma once

#include <iostream>

namespace Dmdb {

class DmdbClientManager;
class DmdbClientContact;
class DmdbRDBManager;
class DmdbEventManager;
class DmdbServerLogger;

struct DmdbRepilcationManagerRequiredComponents {
    bool _is_myself_master;
    DmdbClientManager* _client_manager;
    DmdbRDBManager* _rdb_manager;
    DmdbEventManager* _event_manager;
    DmdbServerLogger* _server_logger;
};

class DmdbReplicationManager {
public:
    virtual void ReplicateDataToSlaves(const std::string &data) = 0;
    virtual void AddReplayOkSize(size_t addLen) = 0;
    virtual long long GetReplayOkSize() = 0;
    virtual DmdbClientContact* GetMasterClientContact() = 0;
    virtual bool IsOneOfMySlaves(DmdbClientContact* client) = 0;
    virtual bool IsMyMaster(const std::string clientName) = 0;
    virtual void SetMyMasterClientContact(DmdbClientContact* master) = 0;
    virtual bool RemoveMasterOrReplica(DmdbClientContact* client) = 0;
    virtual bool FullSyncDataToReplica(DmdbClientContact* client) = 0;
    virtual bool HandleFullSyncOver(int fd, bool isSuccess, bool isDisconnected) = 0;
    virtual void AddReplicaByFd(int fd) = 0;
    virtual bool FullSyncFromMater() = 0;
    virtual void AskReplicaForReplayOkSize(DmdbClientContact* replica) = 0;
    virtual void ReportToMasterMyReplayOkSize() = 0;
    virtual void SetReplicaReplayOkSize(DmdbClientContact* replica, long long size) = 0;
    virtual long long GetReplOffset() = 0;
    virtual void SetReplOffset(long long offset) = 0;
    virtual std::string GetMultiBulkOfReplicasOrMaster() = 0;
    virtual void TimelyTask() = 0;
    virtual void SetMasterPassword(const std::string &pwd) = 0;
    virtual void SetMasterAddrInfo(const std::string &ip, int port) = 0;
    virtual size_t GetReplicaCount() = 0;
    virtual size_t CountNumOfReplicasByOffset() = 0;
    virtual void WaitForNReplicasAck(DmdbClientContact* client, size_t waitNum, uint64_t waitMs) = 0;
    virtual bool StopWaitting(DmdbClientContact* client) = 0;
    virtual bool IsWaitting(DmdbClientContact* client) = 0; 
    virtual ~DmdbReplicationManager();
    void SetFullSyncMaxMs(uint64_t ms);
    void SetTaskInterval(uint64_t interval);
    static DmdbReplicationManager* GenerateReplicationManagerByRole(bool isMaster);
protected:
    DmdbReplicationManager();
    uint64_t _full_sync_max_ms;
    uint64_t _last_timely_exe_ms;
    uint64_t _repl_timely_task_interval;
private:
};

}