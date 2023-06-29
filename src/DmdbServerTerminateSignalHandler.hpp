#pragma once

namespace Dmdb {
    class DmdbServer;
}

class DmdbServerTerminateSignalHandler {
public:
    static void TerminateSignalHandle(int sig);
    static void SetServerInstance(Dmdb::DmdbServer* handleServerInstance);
private:
    static Dmdb::DmdbServer* _handle_server_instance;
};