#pragma once

namespace Dmdb {
    class DmdbReplicaReplicationManager;
};


class DmdbTimerHandlerForBlockingIO {
public:
    static void HandleSignal(int sig);
    static void SetInstance(Dmdb::DmdbReplicaReplicationManager* handleInstance);
private:
    static Dmdb::DmdbReplicaReplicationManager* _handle_instance;
};