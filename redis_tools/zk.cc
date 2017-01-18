#include <iostream>
#include <errno.h>
#include <string>
#include <zookeeper/zookeeper.h>
#include <zookeeper/zookeeper_log.h>


namespace redis {
namespace monitor {


void ZKClient::simple_watcher(zhandle_t * zkh,
                    int type,
                    int state,
                    const char* path,
                    void* context) {
        /*
         *      * zookeeper_init might not have returned, so we
         *      * use zkh instead.
         */     
        if (type == ZOO_SESSION_EVENT) {
            if (state == ZOO_CONNECTED_STATE) {
                connected = true;
                LOG_DEBUG(("Received a connected event."));
            } else if (state == ZOO_CONNECTING_STATE) {
                if(connected == true) {
                    LOG_WARN(("Disconnected."));
                }
                connected = false;
            } else if (state == ZOO_EXPIRED_SESSION_STATE) {
                expired = true;
                connected = false;
                zookeeper_close(zkh);
            }
        }
        LOG_DEBUG(("Event: %s, %d", zktype2string(type), state));
}

void ZKClient::create_node_completion(int rc, const char * value, const void * data) {
    switch (rc) {
        case ZCONNECTIONLOSS:
            LOG_ERROR("create node failed, have retried " + itoa(node_create_retried_times_++));
            if (node_create_retried_times_ >= 10) {
                std::exit(1);
            } 
            create_node_completion(value, (const char *) data);
            break;
        case ZOK:
            LOG_INFO(("Created parent node", value));
            break;
        case ZNODEEXISTS:
            LOG_WARN(("Node already exists"));
            break;
        default:
            LOG_ERROR(("Something went wrong when running for master: %s, %s", value, rc2string(rc)));
            break;
        // only increase this when connection lost.
        node_create_retried_times_ = 0;
    }
}

Status ZKClient::Init() {
    zoo_set_debug_level(ZOO_LOG_LEVEL_DEBUG);
    z_handler_  = zookeeper_init(host_port_.c_str(), simple_watcher, 15000, 0, 0, 0); 
    Status global_status = errno;
    return global_status;
}

Status ZKClient::CreateNode(string& path, string& str_val) {
        zoo_acreate(z_handler_,
                    path.c_str(),
                    str_val.c_str(),
                    0,
                    &ZOO_OPEN_ACL_UNSAFE,
                    0,
                    create_node_completion,
                    NULL);
}

class ZKClient {
    typedef int Status;
    ZKClient(const string& hostPort):host_port_(hostPort){}
    Status Init(); 
    Status CreateNode();

  private:
    void simple_watcher(zhandle_t * zkh, int type, int state, const char* path, void* context);
    void create_node_completion(int rc, const char * value, const void * data);
    

    string host_port_;
    zhandle_t * z_handler_{nullptr};
    uint32_t node_create_retried_times_{0};
    bool connected_{false};
    bool expired_{false};
}

} // monitor 
} // redis


int main() {
    
}
