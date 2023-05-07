

#pragma once

namespace {

class DmdbClusterManager {
private:
    long long _cluster_node_timeout; /* Cluster node timeout. */
    std::string cluster_configfile; /* Cluster auto-generated config file name. */
    struct clusterState *_cluster_info; 
    int _cluster_config_file_fd;   /* cluster config fd, will be flock */
};

}