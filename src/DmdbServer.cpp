#include "DmdbServer.hpp"

namespace Dmdb {

DmdbServer* DmdbServer::_self_instance = nullptr;

DmdbServer* DmdbServer::GetUniqueServerInstance() {
    if(_self_instance == nullptr)
        _self_instance = new DmdbServer();
    
    return _self_instance;
}



}