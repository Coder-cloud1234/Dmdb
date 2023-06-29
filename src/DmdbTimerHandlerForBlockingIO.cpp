#include "DmdbTimerHandlerForBlockingIO.hpp"
#include "DmdbReplicationManager.hpp"
#include "DmdbReplicaReplicationManager.hpp"

/*
template <class T>
T* DmdbTimerHandlerForBlockingIO<T>::_handle_instance = nullptr;

template <class T>
void DmdbTimerHandlerForBlockingIO<T>::SetInstance(T* handleInstance) {
    _handle_instance = handleInstance;
}

template <class T>
void DmdbTimerHandlerForBlockingIO<T>::HandleSignal(int sig) {
    _handle_instance->HandleSignal(sig);
}
*/


Dmdb::DmdbReplicaReplicationManager* DmdbTimerHandlerForBlockingIO::_handle_instance = nullptr;

void DmdbTimerHandlerForBlockingIO::SetInstance(Dmdb::DmdbReplicaReplicationManager* handleInstance) {
    _handle_instance = handleInstance;
}

void DmdbTimerHandlerForBlockingIO::HandleSignal(int sig) {
    _handle_instance->HandleSignal(sig);
}