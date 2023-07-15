#include "DmdbServer.hpp"

namespace Dmdb{
DmdbServer* serverInstance = nullptr; 
}

int main(int argc, char**argv) {
    std::string configFile = "";
    if(argc > 2) {
        std::cout << "Too many arguments!" << std::endl;
        exit(0);
    } else if(argc == 2) {
        configFile = argv[1];
    }
    Dmdb::serverInstance = Dmdb::DmdbServer::GetUniqueServerInstance(configFile);
    Dmdb::serverInstance->DoService();
    return 0;
}
