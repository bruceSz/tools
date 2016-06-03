#include <iostream>
#include <string>
#include <memory>

#include <stdlib.h>

#include "kudu/client/client.h"

using namespace kudu::client;

void Usage(char** argv){
  std::cout << argv[0] << ": master_addr [host:port] action" << std::endl;
  std::cout << "\t" << "action:" << std::endl;
  std::cout << "\t\t" << "list_tables" << std::endl;
  exit(1);
}
void dispatch_command(std::string command ){
  //TODO:

}

int main(int argc, char** argv){
  if (argc != 3){
    Usage(argv);
  }
  std::string master_addr(argv[1]);
  std::cout << "Master addr: " << master_addr << std::endl;

  std::shared_ptr<kudu::client::KuduClient> client;


}
