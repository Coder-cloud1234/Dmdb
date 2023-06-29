#pragma once

#include <string>
#include <list>
#include <unordered_map>

#include "DmdbReplicationManager.hpp"



namespace Dmdb {

class DmdbClientContact;

struct ReplicaSupplementary {
    long long _replay_ok_size;
};

struct WaitInfoOfClient {
    size_t _wait_replica_ack_num;
    uint64_t _wait_ms;
};

class DmdbMasterReplicationManager : public DmdbReplicationManager {
public:
    virtual DmdbClientContact* GetMasterClientContact();
    virtual void ReplicateDataToSlaves(const std::string &data);
    virtual bool IsOneOfMySlaves(DmdbClientContact* client);
    virtual bool IsMyMaster(const std::string clientName);
    virtual void SetMyMasterClientContact(DmdbClientContact* master);
    virtual void SetReplayOkSize(size_t addLen);
    virtual long long GetReplayOkSize();
    virtual bool RemoveMasterOrReplica(DmdbClientContact* client);
    virtual void AddReplicaByFd(int fd);
    virtual bool FullSyncDataToReplica(DmdbClientContact* client);
    virtual bool HandleFullSyncOver(int fd, bool isSuccess, bool isDisconnected);
    virtual bool FullSyncFromMater();
    virtual void AskReplicaForReplayOkSize(DmdbClientContact* replica);
    virtual void ReportToMasterMyReplayOkSize();
    virtual void SetReplicaReplayOkSize(DmdbClientContact* replica, long long size);
    virtual long long GetReplOffset();
    virtual std::string GetMultiBulkOfReplicasOrMaster();
    virtual void TimelyTask();
    virtual void SetMasterPassword(const std::string &pwd);
    virtual void SetMasterAddrInfo(const std::string &ip, int port);
    virtual size_t GetReplicaCount();
    virtual void WaitForNReplicasAck(DmdbClientContact* client, size_t waitNum, uint64_t waitMs);
    virtual bool StopWaitting(DmdbClientContact* client);
    virtual bool IsWaitting(DmdbClientContact* client);    
    size_t CountNumOfReplicasByOffset();
    void CheckWaittingClients(uint64_t currentMs); 
    DmdbMasterReplicationManager();
    virtual ~DmdbMasterReplicationManager();
private:
    void GenerateRelicationID();
    void ReplyAllBufferToReplica(DmdbClientContact* client);
    std::list<DmdbClientContact*> _replicas;
    std::unordered_map<DmdbClientContact*, ReplicaSupplementary> _replicas_supplementary;
    std::unordered_map<DmdbClientContact*, WaitInfoOfClient> _clients_wait_n_replicas;
    std::string _current_replication_id;
    long long _current_repl_offset;
    long long _last_sample_repl_offset;
    /*
    std::string _last_replication_id;
    long long _repl_offset_from_last_replid;
    std::string _repl_buffer;
    long long _repl_buffer_size;
    long long _repl_buffer_used;
    long long _repl_buffer_to_write_pos;
    long long _buffer_pos_in_repl_offset;
    int _repl_slaves_min_num_to_write;
    int _repl_slaves_ok_num;
    */
};

}