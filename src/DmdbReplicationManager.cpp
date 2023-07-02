#include "DmdbReplicationManager.hpp"
#include "DmdbMasterReplicationManager.hpp"
#include "DmdbReplicaReplicationManager.hpp"
#include "DmdbServerFriends.hpp"
#include "DmdbRDBManager.hpp"
#include "DmdbClientManager.hpp"

namespace Dmdb {

DmdbReplicationManager::DmdbReplicationManager() {

}

DmdbReplicationManager::~DmdbReplicationManager() {

}

DmdbReplicationManager* DmdbReplicationManager::GenerateReplicationManagerByRole(bool isMaster) {
    if(isMaster) {
        return new DmdbMasterReplicationManager();
    } else {
        return new DmdbReplicaReplicationManager();
    }
}

void DmdbReplicationManager::SetFullSyncMaxMs(uint64_t ms) {
    _full_sync_max_ms = ms;
}

void DmdbReplicationManager::SetTaskInterval(uint64_t interval) {
    _repl_timely_task_interval = interval;
}

}