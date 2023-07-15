#include "DmdbMasterReplicationManager.hpp"
#include "DmdbClientContact.hpp"
#include "DmdbUtil.hpp"
#include "DmdbServerFriends.hpp"
#include "DmdbClientManager.hpp"
#include "DmdbRDBManager.hpp"
#include "DmdbEventManager.hpp"
#include "DmdbEventManagerCommon.hpp"
#include "DmdbEventProcessor.hpp"

namespace Dmdb {

void DmdbMasterReplicationManager::GenerateRelicationID() {
    _current_replication_id = std::to_string(DmdbUtil::GetCurrentMs());
    for(uint8_t i = 0; i < 10; ++i) {
        _current_replication_id.push_back(rand()%26+'a');
    }
}   

DmdbClientContact* DmdbMasterReplicationManager::GetMasterClientContact() {
    return nullptr;
}

void DmdbMasterReplicationManager::ReplicateDataToSlaves(const std::string &data) {
    for(std::list<DmdbClientContact*>::iterator it = _replicas.begin() ; it != _replicas.end(); ++it) {
        (*it)->AddReplyData2Client(data);
    }
    _current_repl_offset += data.length();
}

bool DmdbMasterReplicationManager::IsOneOfMySlaves(DmdbClientContact* client) {
    for(std::list<DmdbClientContact*>::iterator it = _replicas.begin() ; it != _replicas.end(); ++it) {
        if(*it == client) {
            return true;
        }
    }    
    return false;
}

bool DmdbMasterReplicationManager::IsMyMaster(const std::string clientName) {
    return false;
}

void DmdbMasterReplicationManager::SetMyMasterClientContact(DmdbClientContact* master) {

}

void DmdbMasterReplicationManager::AddReplayOkSize(size_t addLen) {

}

bool DmdbMasterReplicationManager::RemoveMasterOrReplica(DmdbClientContact* client) {
    for(std::list<DmdbClientContact*>::iterator it = _replicas.begin(); it != _replicas.end(); ++it) {
        if(*it == client) {
            _replicas.erase(it);
            if(_replicas_supplementary.find(client) != _replicas_supplementary.end()) {
                _replicas_supplementary.erase(client);
            }
            return true;
        }
    }    
    return false;    
}

void DmdbMasterReplicationManager::ReplyAllBufferToReplica(DmdbClientContact* client) {
    DmdbRepilcationManagerRequiredComponents components;
    GetDmdbRepilcationManagerRequiredComponents(components);    
    size_t bufLen = client->GetOutputBufLength(), writePos = 0;
    while(writePos < bufLen) {
        int ret = write(client->GetClientSocket(), client->GetOutputBuf()+writePos, bufLen-writePos);
        if(ret < 0) {
            components._client_manager->DisconnectClient(client->GetClientSocket());
        }
        writePos += ret;       
    }
    client->ClearRepliedData(writePos);
}

bool DmdbMasterReplicationManager::FullSyncDataToReplica(DmdbClientContact* client) {
    DmdbRepilcationManagerRequiredComponents components;
    GetDmdbRepilcationManagerRequiredComponents(components);

    /* We should delete these events to avoid affecting rdb child sending data to replica */
    components._event_manager->DelEvent4Fd(client->GetClientSocket(), EpollEvent::OUT);
    components._event_manager->DelEvent4Fd(client->GetClientSocket(), EpollEvent::IN);

    /* We have removed all events of client's socket, before we set replication plan, we need to reply all the remaining data 
     * in the replica's output buffer to the replica */
    ReplyAllBufferToReplica(client);

    _replicas.push_back(client);
    components._rdb_manager->SetBackgroundSavePlan(client->GetClientSocket(), client->GetClientSocket());

    return true;
}

bool DmdbMasterReplicationManager::HandleFullSyncOver(int fd, bool isSuccess, bool isDisconnected) {
    DmdbRepilcationManagerRequiredComponents components;
    GetDmdbRepilcationManagerRequiredComponents(components);

    /* We can only receive data from the replica during the period that full sync is finished but the replication offset hasn't
     * be received. If we add OUT event now, it may mix the accumulative commands when syncing and the RDB data sent at last. 
     * We can add OUT event for the replica only after we receive its replication offset. */
    if(isSuccess) {
        components._event_manager->AddEvent4Fd(fd, EpollEvent::IN, EventProcessorType::INTERACT);
        return true;
    }
    
    if(isDisconnected) {
        /* DisconnectClient will check if the client of fd is one of my replicas, if so, it will remove the client from _replicas */
        components._client_manager->DisconnectClient(fd);
        return false;
    }

    return false; 
}

void DmdbMasterReplicationManager::AddReplicaByFd(int fd) {
    DmdbRepilcationManagerRequiredComponents components;
    GetDmdbRepilcationManagerRequiredComponents(components);
    DmdbClientContact* replica = components._client_manager->GetClientContactByFd(fd);
    if(replica) {
        _replicas.push_back(replica);
    }    
}

bool DmdbMasterReplicationManager::FullSyncFromMater() {
    return false;
}

long long DmdbMasterReplicationManager::GetReplayOkSize() {
    return 0;
}

/* "replica == nullptr" means asking all the replicas for _replay_ok_size */
void DmdbMasterReplicationManager::AskReplicaForReplayOkSize(DmdbClientContact* replica) {
    std::string command = "*2\r\n";
    command += "$8\r\nreplconf\r\n";
    command += "$6\r\ngetack\r\n";
    for(std::list<DmdbClientContact*>::iterator it = _replicas.begin(); it != _replicas.end(); ++it) {
        if(replica == nullptr) {
            (*it)->AddReplyData2Client(command);
            continue;
        }
        if(*it == replica) {
            (*it)->AddReplyData2Client(command);
            break;
        }
    }    
}

void DmdbMasterReplicationManager::ReportToMasterMyReplayOkSize() {

}

void DmdbMasterReplicationManager::SetReplicaReplayOkSize(DmdbClientContact* replica, long long size) {
    /* This means this replica finished full sync and reported replication offset just now. 
     * Then we can write to commands to it. */
    if(_replicas_supplementary.find(replica) == _replicas_supplementary.end()) {
        DmdbRepilcationManagerRequiredComponents components;
        GetDmdbRepilcationManagerRequiredComponents(components);        
        components._event_manager->AddEvent4Fd(replica->GetClientSocket(), EpollEvent::OUT, EventProcessorType::INTERACT);
    }
    _replicas_supplementary[replica]._replay_ok_size = size;
}

long long DmdbMasterReplicationManager::GetReplOffset() {
    return _current_repl_offset;
}

void DmdbMasterReplicationManager::SetReplOffset(long long offset) {
    _current_repl_offset = offset;
}

std::string DmdbMasterReplicationManager::GetMultiBulkOfReplicasOrMaster() {
    std::string multiBulk = "*" + std::to_string(_replicas.size()) + "\r\n";
    for(std::list<DmdbClientContact*>::iterator it = _replicas.begin(); it != _replicas.end(); ++it) {
        bool hasSupp = _replicas_supplementary.find(*it) != _replicas_supplementary.end();
        if(hasSupp)
            multiBulk += "*3\r\n";
        else
            multiBulk += "*2\r\n";
        multiBulk += "$" + std::to_string((*it)->GetIp().length()) + "\r\n";
        multiBulk += (*it)->GetIp() + "\r\n";
        multiBulk += "$" + std::to_string(std::to_string((*it)->GetPort()).length()) + "\r\n";
        multiBulk += std::to_string((*it)->GetPort()) + "\r\n";
        if(hasSupp) {
            multiBulk += "$" + std::to_string(std::to_string(_replicas_supplementary[*it]._replay_ok_size).length()) + "\r\n";
            multiBulk += std::to_string(_replicas_supplementary[*it]._replay_ok_size) + "\r\n";
        }
    }
    return multiBulk;    
}

void DmdbMasterReplicationManager::TimelyTask() {
    uint64_t currentMs = DmdbUtil::GetCurrentMs();
    if(currentMs - _last_timely_exe_ms < _repl_timely_task_interval) {
        return;
    }
    CheckWaittingClients(currentMs);
    _last_sample_repl_offset = _current_repl_offset;
}

void DmdbMasterReplicationManager::SetMasterPassword(const std::string &pwd) {

}

void DmdbMasterReplicationManager::SetMasterAddrInfo(const std::string &ip, int port) {

}

size_t DmdbMasterReplicationManager::GetReplicaCount() {
    return _replicas.size();
}

void DmdbMasterReplicationManager::CheckWaittingClients(uint64_t currentMs) {
    std::unordered_map<DmdbClientContact*, WaitInfoOfClient>::iterator it = _clients_wait_n_replicas.begin();
    size_t num = CountNumOfReplicasByOffset();
    std::vector<DmdbClientContact*> waittingOverClients; 
    for(; it != _clients_wait_n_replicas.end(); it++) {
        if(num >= it->second._wait_replica_ack_num || (it->second._wait_ms != 0 && currentMs >= it->second._wait_ms)) {
            it->first->AddReplyData2Client(":" + std::to_string(num) + "\r\n");
            waittingOverClients.emplace_back(it->first);
        }
    }
    for(size_t i = 0; i < waittingOverClients.size(); ++i) {
        _clients_wait_n_replicas.erase(waittingOverClients[i]);
    }
}

bool DmdbMasterReplicationManager::IsWaitting(DmdbClientContact* client) {
    return _clients_wait_n_replicas.find(client) != _clients_wait_n_replicas.end();
}

size_t DmdbMasterReplicationManager::CountNumOfReplicasByOffset() {
    size_t num = 0;
    std::unordered_map<DmdbClientContact*, ReplicaSupplementary>::iterator it = _replicas_supplementary.begin();
    for(; it != _replicas_supplementary.end(); it++) {
        if(it->second._replay_ok_size >= _last_sample_repl_offset) {
            num++;
        }
    }
    return num;
}

void DmdbMasterReplicationManager::WaitForNReplicasAck(DmdbClientContact* client, size_t waitNum, uint64_t waitMs) {
    _clients_wait_n_replicas[client]._wait_replica_ack_num = waitNum;
    _clients_wait_n_replicas[client]._wait_ms = waitMs==0 ? waitMs:DmdbUtil::GetCurrentMs()+waitMs;
    AskReplicaForReplayOkSize(nullptr);
}

bool DmdbMasterReplicationManager::StopWaitting(DmdbClientContact* client) {
    std::unordered_map<DmdbClientContact*, WaitInfoOfClient>::iterator it = _clients_wait_n_replicas.find(client);
    if(it != _clients_wait_n_replicas.end()) {
        _clients_wait_n_replicas.erase(it);
        return true;
    }
    return false;
} 

DmdbMasterReplicationManager::DmdbMasterReplicationManager() {
    _full_sync_max_ms = 60*1000;
    _last_timely_exe_ms = 0;
    _repl_timely_task_interval = 1000;
    _current_repl_offset = 0;
    _last_sample_repl_offset = 0;
}

DmdbMasterReplicationManager::~DmdbMasterReplicationManager() {

}

}

