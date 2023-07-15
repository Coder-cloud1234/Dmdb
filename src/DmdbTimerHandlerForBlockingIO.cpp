#include "DmdbTimerHandlerForBlockingIO.hpp"
#include "DmdbReplicationManager.hpp"
#include "DmdbReplicaReplicationManager.hpp"


Dmdb::DmdbReplicaReplicationManager* DmdbTimerHandlerForBlockingIO::_handle_instance = nullptr;

void DmdbTimerHandlerForBlockingIO::SetInstance(Dmdb::DmdbReplicaReplicationManager* handleInstance) {
    _handle_instance = handleInstance;
}

void DmdbTimerHandlerForBlockingIO::HandleSignal(int sig) {
    _handle_instance->HandleSignal(sig);
}