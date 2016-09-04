
#include <unistd.h>
#include <arpa/inet.h>
#include <netdb.h>
#include "util.h"
#include <iostream>

using namespace kudu::client;
using kudu::Status;

using namespace std;

std::string getLocalIp(){
  char name[100];
  gethostname(name, 100);
  hostent * ht = gethostbyname(name);
  if (ht == NULL){
    std::cerr << "can't get host by name: " << name << std::endl;
    exit(1);
  }
  struct in_addr ** addr_list = (struct in_addr**) ht->h_addr_list;
  for (int i=0; addr_list[i] != NULL; i++){
    std::string ip(inet_ntoa(*addr_list[i]));
    if (ip.find("172")==0){
      return ip;
    }
  }
}

std::tr1::shared_ptr<kudu::client::KuduClient> create_client(std::string master_addr) {
  std::tr1::shared_ptr<kudu::client::KuduClient> client;
  {
    Status s = KuduClientBuilder().add_master_server_addr(master_addr).Build(&client);
    if (! s.ok()) {
      std::cerr << "cannot connect to master" << endl;
      exit(1);
    }
  }
  return client;

}

std::tr1::shared_ptr<KuduTable> openTable(std::tr1::shared_ptr<kudu::client::KuduClient> client, 
                                std::string tab_name) {
  
  std::tr1::shared_ptr<KuduTable> table;
  {
    Status s = client->OpenTable(tab_name, &table);
    if (!s.ok()) {
      cerr << "Open table error " 
          << s.ToString() << endl;
      exit(1);
    }
  }
  return table;
}
