#pragma once

namespace Dmdb {
    class DmdbReplicaReplicationManager;
};

/*
template <class T> 
class DmdbTimerHandlerForBlockingIO {
public:
    static void HandleSignal(int sig);
    static void SetInstance(T* handleInstance);
private:
    static T* _handle_instance;
};
*/

class DmdbTimerHandlerForBlockingIO {
public:
    static void HandleSignal(int sig);
    static void SetInstance(Dmdb::DmdbReplicaReplicationManager* handleInstance);
private:
    static Dmdb::DmdbReplicaReplicationManager* _handle_instance;
};