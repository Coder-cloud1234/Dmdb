#include "DmdbServerTerminateSignalHandler.hpp"
#include "DmdbServer.hpp"


Dmdb::DmdbServer* DmdbServerTerminateSignalHandler::_handle_server_instance = nullptr;

void DmdbServerTerminateSignalHandler::SetServerInstance(Dmdb::DmdbServer* handleServerInstance) {
    _handle_server_instance = handleServerInstance;
}

void DmdbServerTerminateSignalHandler::TerminateSignalHandle(int sig) {
    _handle_server_instance->ShutdownServerBySignal(sig);
}

