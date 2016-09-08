#include <string>
#include <stdlib.h>

#include "kudu/client/client.h"

#define KUDU_EXIT_NOT_OK(s, msg) do {\
    ::kudu::Status _s = (s);\
    if (PREDICT_FALSE(!_s.ok())) {\
      std::cout << msg << "::" << _s.ToString()  << endl;\
      exit(1);\
    };\
  } while(0);

#define JD_LOG_OUTPUT_NOT_OK(s,msg) do {\
    ::kudu::Status _s = (s);\
    if (PREDICT_FALSE(!_s.ok())) {\
      std::cout << msg << "::" <<  _s.ToString()  << endl;\
    };\
    } while(0);

std::string getLocalIp();
std::tr1::shared_ptr<kudu::client::KuduClient> create_client(std::string master_addr);
std::tr1::shared_ptr<kudu::client::KuduTable> openTable(std::tr1::shared_ptr<kudu::client::KuduClient> client,std::string tab_name);


inline int stringToInt32(std::string s) {
    return std::stoi(s, nullptr);
}

inline long long stringToInt64(std::string s) {
    return strtoll(s.c_str(), NULL,10);
}


