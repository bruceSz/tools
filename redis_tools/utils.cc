

namespace redis {
namespace monitor {

const char* zktype2string(int type){
        if (type == ZOO_CREATED_EVENT)
            return "CREATED_EVENT";
        if (type == ZOO_DELETED_EVENT)
            return "DELETED_EVENT";
        if (type == ZOO_CHANGED_EVENT)
            return "CHANGED_EVENT";
        if (type == ZOO_CHILD_EVENT)
            return "CHILD_EVENT";
        if (type == ZOO_SESSION_EVENT)
            return "SESSION_EVENT";
        if (type == ZOO_NOTWATCHING_EVENT)
            return "NOTWATCHING_EVENT";
        return "UNKNOWN_EVENT_TYPE";
}



} // monitor
} // redis
