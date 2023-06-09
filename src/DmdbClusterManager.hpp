#pragma once

#include <string>

namespace Dmdb {
struct ClusterState {

};

class DmdbClusterManager {
private:
    long long _cluster_node_timeout;  
    ClusterState *_cluster_info;
    std::string cluster_configfile; 
    int _cluster_config_file_fd;
    int _port_for_cluster;
    int _bind_ip_fd_for_cluster;
};

}